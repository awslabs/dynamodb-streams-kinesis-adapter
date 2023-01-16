package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.internal.BlockedOnParentShardException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.AsynchronousGetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.IDataFetcher;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisConsumerStates;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.IShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibLeaseCoordinator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.MetricsCollectingTaskDecorator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SequenceNumberValidator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownNotification;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.StreamConfig;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SynchronousGetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.TaskResult;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.LeaseCleanupManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

public class DynamoDBStreamsShardConsumer implements IShardConsumer {

    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsShardConsumer.class);

    private final StreamConfig streamConfig;
    private final IRecordProcessor recordProcessor;
    private final KinesisClientLibConfiguration config;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ExecutorService executorService;
    private final ShardInfo shardInfo;
    private final DynamoDBStreamsDataFetcher dataFetcher;
    private final IMetricsFactory metricsFactory;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private ICheckpoint checkpoint;
    private LeaseCleanupManager leaseCleanupManager;
    // Backoff time when polling to check if application has finished processing parent shards
    private final long parentShardPollIntervalMillis;
    private final boolean cleanupLeasesOfCompletedShards;
    private final long taskBackoffTimeMillis;
    private final boolean skipShardSyncAtWorkerInitializationIfLeasesExist;

    private final ShardSyncer shardSyncer;

    private ITask currentTask;
    private long currentTaskSubmitTime;
    private Future<TaskResult> future;
    private ShardSyncStrategy shardSyncStrategy;

    private final GetRecordsCache getRecordsCache;


    public ShardSyncer getShardSyncer() {
        return shardSyncer;
    }

    public GetRecordsCache getGetRecordsCache() {
        return getRecordsCache;
    }

    private static final GetRecordsRetrievalStrategy makeStrategy(IDataFetcher dataFetcher,
                                                                  Optional<Integer> retryGetRecordsInSeconds,
                                                                  Optional<Integer> maxGetRecordsThreadPool,
                                                                  ShardInfo shardInfo) {
        Optional<GetRecordsRetrievalStrategy> getRecordsRetrievalStrategy = retryGetRecordsInSeconds.flatMap(retry ->
                maxGetRecordsThreadPool.map(max ->
                        new AsynchronousGetRecordsRetrievalStrategy(dataFetcher, retry, max, shardInfo.getShardId())));

        return getRecordsRetrievalStrategy.orElse(new SynchronousGetRecordsRetrievalStrategy(dataFetcher));
    }

    /*
     * Tracks current state. It is only updated via the consumeStream/shutdown APIs. Therefore we don't do
     * much coordination/synchronization to handle concurrent reads/updates.
     */
    private DynamoDBStreamsConsumerStates.ConsumerState currentState = DynamoDBStreamsConsumerStates.INITIAL_STATE;
    /*
     * Used to track if we lost the primary responsibility. Once set to true, we will start shutting down.
     * If we regain primary responsibility before shutdown is complete, Worker should create a new ShardConsumer object.
     */
    private volatile ShutdownReason shutdownReason;
    private volatile ShutdownNotification shutdownNotification;

    /**
     * @param shardInfo Shard information
     * @param streamConfig Stream configuration to use
     * @param checkpoint Checkpoint tracker
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param config Kinesis library configuration
     * @param leaseCoordinator Used to manage leases for current worker
     * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
     * @param executorService ExecutorService used to execute process tasks for this shard
     * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
     * @param backoffTimeMillis backoff interval when we encounter exceptions
     * @param shardSyncer shardSyncer instance used to check and create new leases
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    @Deprecated
    DynamoDBStreamsShardConsumer(ShardInfo shardInfo,
                                 StreamConfig streamConfig,
                                 ICheckpoint checkpoint,
                                 IRecordProcessor recordProcessor,
                                 KinesisClientLibLeaseCoordinator leaseCoordinator,
                                 long parentShardPollIntervalMillis,
                                 boolean cleanupLeasesOfCompletedShards,
                                 ExecutorService executorService,
                                 IMetricsFactory metricsFactory,
                                 long backoffTimeMillis,
                                 boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                 KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy) {

        this(shardInfo,
                streamConfig,
                checkpoint,
                recordProcessor,
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesOfCompletedShards,
                executorService,
                metricsFactory,
                backoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                Optional.empty(),
                Optional.empty(),
                config, shardSyncer, shardSyncStrategy);
    }

    /**
     * @param shardInfo Shard information
     * @param streamConfig Stream configuration to use
     * @param checkpoint Checkpoint tracker
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param leaseCoordinator Used to manage leases for current worker
     * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
     * @param executorService ExecutorService used to execute process tasks for this shard
     * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
     * @param backoffTimeMillis backoff interval when we encounter exceptions
     * @param retryGetRecordsInSeconds time in seconds to wait before the worker retries to get a record.
     * @param maxGetRecordsThreadPool max number of threads in the getRecords thread pool.
     * @param config Kinesis library configuration
     * @param shardSyncer shardSyncer instance used to check and create new leases
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    @Deprecated
    DynamoDBStreamsShardConsumer(ShardInfo shardInfo,
                                 StreamConfig streamConfig,
                                 ICheckpoint checkpoint,
                                 IRecordProcessor recordProcessor,
                                 KinesisClientLibLeaseCoordinator leaseCoordinator,
                                 long parentShardPollIntervalMillis,
                                 boolean cleanupLeasesOfCompletedShards,
                                 ExecutorService executorService,
                                 IMetricsFactory metricsFactory,
                                 long backoffTimeMillis,
                                 boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                 Optional<Integer> retryGetRecordsInSeconds,
                                 Optional<Integer> maxGetRecordsThreadPool,
                                 KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy) {
        this(
                shardInfo,
                streamConfig,
                checkpoint,
                recordProcessor,
                new RecordProcessorCheckpointer(
                        shardInfo,
                        checkpoint,
                        new SequenceNumberValidator(
                                streamConfig.getStreamProxy(),
                                shardInfo.getShardId(),
                                streamConfig.shouldValidateSequenceNumberBeforeCheckpointing()),
                        metricsFactory),
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesOfCompletedShards,
                executorService,
                metricsFactory,
                backoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                new DynamoDBStreamsDataFetcher(streamConfig.getStreamProxy(), shardInfo),
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                config, shardSyncer, shardSyncStrategy
        );
    }

    /**
     * @param shardInfo Shard information
     * @param streamConfig Stream Config to use
     * @param checkpoint Checkpoint tracker
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param recordProcessorCheckpointer RecordProcessorCheckpointer to use to checkpoint progress
     * @param leaseCoordinator Used to manage leases for current worker
     * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
     * @param cleanupLeasesOfCompletedShards  clean up the leases of completed shards
     * @param executorService ExecutorService used to execute process tasks for this shard
     * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
     * @param backoffTimeMillis backoff interval when we encounter exceptions
     * @param skipShardSyncAtWorkerInitializationIfLeasesExist Skip sync at init if lease exists
     * @param dynamoDBStreamsDataFetcher  to fetch data from Dynamo DB streams.
     * @param retryGetRecordsInSeconds time in seconds to wait before the worker retries to get a record
     * @param maxGetRecordsThreadPool max number of threads in the getRecords thread pool
     * @param config Kinesis library configuration
     * @param shardSyncer shardSyncer instance used to check and create new leases
     */
    @Deprecated
    DynamoDBStreamsShardConsumer(ShardInfo shardInfo,
                                 StreamConfig streamConfig,
                                 ICheckpoint checkpoint,
                                 IRecordProcessor recordProcessor,
                                 RecordProcessorCheckpointer recordProcessorCheckpointer,
                                 KinesisClientLibLeaseCoordinator leaseCoordinator,
                                 long parentShardPollIntervalMillis,
                                 boolean cleanupLeasesOfCompletedShards,
                                 ExecutorService executorService,
                                 IMetricsFactory metricsFactory,
                                 long backoffTimeMillis,
                                 boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                 DynamoDBStreamsDataFetcher dynamoDBStreamsDataFetcher,
                                 Optional<Integer> retryGetRecordsInSeconds,
                                 Optional<Integer> maxGetRecordsThreadPool,
                                 KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy) {

        this(shardInfo, streamConfig, checkpoint, recordProcessor, recordProcessorCheckpointer, leaseCoordinator,
                parentShardPollIntervalMillis, cleanupLeasesOfCompletedShards, executorService, metricsFactory,
                backoffTimeMillis, skipShardSyncAtWorkerInitializationIfLeasesExist, dynamoDBStreamsDataFetcher, retryGetRecordsInSeconds,
                maxGetRecordsThreadPool, config, shardSyncer, shardSyncStrategy, LeaseCleanupManager.newInstance(streamConfig.getStreamProxy(), leaseCoordinator.getLeaseManager(),
                        Executors.newSingleThreadScheduledExecutor(), metricsFactory, config.shouldCleanupLeasesUponShardCompletion(),
                        config.leaseCleanupIntervalMillis(), config.completedLeaseCleanupThresholdMillis(),
                        config.garbageLeaseCleanupThresholdMillis(), config.getMaxRecords()));
    }

    /**
     * @param shardInfo Shard information
     * @param streamConfig Stream Config to use
     * @param checkpoint Checkpoint tracker
     * @param recordProcessor Record processor used to process the data records for the shard
     * @param recordProcessorCheckpointer RecordProcessorCheckpointer to use to checkpoint progress
     * @param leaseCoordinator Used to manage leases for current worker
     * @param parentShardPollIntervalMillis Wait for this long if parent shards are not done (or we get an exception)
     * @param cleanupLeasesOfCompletedShards  clean up the leases of completed shards
     * @param executorService ExecutorService used to execute process tasks for this shard
     * @param metricsFactory IMetricsFactory used to construct IMetricsScopes for this shard
     * @param backoffTimeMillis backoff interval when we encounter exceptions
     * @param skipShardSyncAtWorkerInitializationIfLeasesExist Skip sync at init if lease exists
     * @param dynamoDBStreamsDataFetcher KinesisDataFetcher to fetch data from Kinesis streams.
     * @param retryGetRecordsInSeconds time in seconds to wait before the worker retries to get a record
     * @param maxGetRecordsThreadPool max number of threads in the getRecords thread pool
     * @param config Kinesis library configuration
     * @param shardSyncer shardSyncer instance used to check and create new leases
     * @param leaseCleanupManager used to clean up leases in lease table.
     */
    DynamoDBStreamsShardConsumer(ShardInfo shardInfo,
                                 StreamConfig streamConfig,
                                 ICheckpoint checkpoint,
                                 IRecordProcessor recordProcessor,
                                 RecordProcessorCheckpointer recordProcessorCheckpointer,
                                 KinesisClientLibLeaseCoordinator leaseCoordinator,
                                 long parentShardPollIntervalMillis,
                                 boolean cleanupLeasesOfCompletedShards,
                                 ExecutorService executorService,
                                 IMetricsFactory metricsFactory,
                                 long backoffTimeMillis,
                                 boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                 DynamoDBStreamsDataFetcher dynamoDBStreamsDataFetcher,
                                 Optional<Integer> retryGetRecordsInSeconds,
                                 Optional<Integer> maxGetRecordsThreadPool,
                                 KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy,
                                 LeaseCleanupManager leaseCleanupManager) {
        this.shardInfo = shardInfo;
        this.streamConfig = streamConfig;
        this.checkpoint = checkpoint;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.leaseCoordinator = leaseCoordinator;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.executorService = executorService;
        this.metricsFactory = metricsFactory;
        this.taskBackoffTimeMillis = backoffTimeMillis;
        this.skipShardSyncAtWorkerInitializationIfLeasesExist = skipShardSyncAtWorkerInitializationIfLeasesExist;
        this.config = config;
        this.dataFetcher = dynamoDBStreamsDataFetcher;
        this.getRecordsCache = config.getRecordsFetcherFactory().createRecordsFetcher(
                makeStrategy(this.dataFetcher, retryGetRecordsInSeconds, maxGetRecordsThreadPool, this.shardInfo),
                this.getShardInfo().getShardId(), this.metricsFactory, this.config.getMaxRecords());
        this.shardSyncer = shardSyncer;
        this.shardSyncStrategy = shardSyncStrategy;
        this.leaseCleanupManager = leaseCleanupManager;
    }

    /**
     * No-op if current task is pending, otherwise submits next task for this shard.
     * This method should NOT be called if the ShardConsumer is already in SHUTDOWN_COMPLETED state.
     *
     * @return true if a new process task was submitted, false otherwise
     */
    public synchronized boolean consumeShard() {
        return checkAndSubmitNextTask();
    }

    private boolean readyForNextTask() {
        return future == null || future.isCancelled() || future.isDone();
    }

    private synchronized boolean checkAndSubmitNextTask() {
        boolean submittedNewTask = false;
        if (readyForNextTask()) {
            TaskOutcome taskOutcome = TaskOutcome.NOT_COMPLETE;
            if (future != null && future.isDone()) {
                taskOutcome = determineTaskOutcome();
            }

            updateState(taskOutcome);
            ITask nextTask = getNextTask();
            if (nextTask != null) {
                currentTask = nextTask;
                try {
                    future = executorService.submit(currentTask);
                    currentTaskSubmitTime = System.currentTimeMillis();
                    submittedNewTask = true;
                    LOG.debug("Submitted new " + currentTask.getTaskType()
                            + " task for shard " + shardInfo.getShardId());
                } catch (RejectedExecutionException e) {
                    LOG.info(currentTask.getTaskType() + " task was not accepted for execution.", e);
                } catch (RuntimeException e) {
                    LOG.info(currentTask.getTaskType() + " task encountered exception ", e);
                }
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("No new task to submit for shard %s, currentState %s",
                            shardInfo.getShardId(),
                            currentState.toString()));
                }
            }
        } else {
            final long timeElapsed = System.currentTimeMillis() - currentTaskSubmitTime;
            final String commonMessage = String.format("Previous %s task still pending for shard %s since %d ms ago. ",
                    currentTask.getTaskType(), shardInfo.getShardId(), timeElapsed);
            if (LOG.isDebugEnabled()) {
                LOG.debug(commonMessage + "Not submitting new task.");
            }
            config.getLogWarningForTaskAfterMillis().ifPresent(value -> {
                if (timeElapsed > value) {
                    LOG.warn(commonMessage);
                }
            });
        }

        return submittedNewTask;
    }

    public boolean isSkipShardSyncAtWorkerInitializationIfLeasesExist() {
        return skipShardSyncAtWorkerInitializationIfLeasesExist;
    }

    private TaskOutcome determineTaskOutcome() {
        try {
            TaskResult result = future.get();
            if (result.getException() == null) {
                if (result.isShardEndReached()) {
                    return TaskOutcome.END_OF_SHARD;
                }
                return TaskOutcome.SUCCESSFUL;
            }
            logTaskException(result);
            // This is the case of result with exception
            if (result.isLeaseNotFound()) {
                return TaskOutcome.LEASE_NOT_FOUND;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // Setting future to null so we don't misinterpret task completion status in case of exceptions
            future = null;
        }
        return TaskOutcome.FAILURE;
    }

    private void logTaskException(TaskResult taskResult) {
        if (LOG.isDebugEnabled()) {
            Exception taskException = taskResult.getException();
            if (taskException instanceof BlockedOnParentShardException) {
                // No need to log the stack trace for this exception (it is very specific).
                LOG.debug("Shard " + shardInfo.getShardId() + " is blocked on completion of parent shard.");
            } else {
                LOG.debug("Caught exception running " + currentTask.getTaskType() + " task: ",
                        taskResult.getException());
            }
        }
    }

    /**
     * Requests the shutdown of the this ShardConsumer. This should give the record processor a chance to checkpoint
     * before being shutdown.
     *
     * @param shutdownNotification used to signal that the record processor has been given the chance to shutdown.
     */
    public void notifyShutdownRequested(ShutdownNotification shutdownNotification) {
        this.shutdownNotification = shutdownNotification;
        markForShutdown(ShutdownReason.REQUESTED);
    }

    /**
     * Shutdown this ShardConsumer (including invoking the RecordProcessor shutdown API).
     * This is called by Worker when it loses responsibility for a shard.
     *
     * @return true if shutdown is complete (false if shutdown is still in progress)
     */
    public synchronized boolean beginShutdown() {
        markForShutdown(ShutdownReason.ZOMBIE);
        checkAndSubmitNextTask();

        return isShutdown();
    }

    synchronized void markForShutdown(ShutdownReason reason) {
        // ShutdownReason.ZOMBIE takes precedence over TERMINATE (we won't be able to save checkpoint at end of shard)
        if (shutdownReason == null || shutdownReason.canTransitionTo(reason)) {
            shutdownReason = reason;
        }
    }

    /**
     * Used (by Worker) to check if this ShardConsumer instance has been shutdown
     * RecordProcessor shutdown() has been invoked, as appropriate.
     *
     * @return true if shutdown is complete
     */
    public boolean isShutdown() {
        return currentState.isTerminal();
    }

    /**
     * @return the shutdownReason
     */
    public ShutdownReason getShutdownReason() {
        return shutdownReason;
    }

    /**
     * Figure out next task to run based on current state, task, and shutdown context.
     *
     * @return Return next task to run
     */
    private ITask getNextTask() {
        ITask nextTask = currentState.createTask(this);

        if (nextTask == null) {
            return null;
        } else {
            return new MetricsCollectingTaskDecorator(nextTask, metricsFactory);
        }
    }

    /**
     * Note: This is a private/internal method with package level access solely for testing purposes.
     * Update state based on information about: task success, current state, and shutdown info.
     *
     * @param taskOutcome The outcome of the last task
     */
    void updateState(TaskOutcome taskOutcome) {
        if (taskOutcome == TaskOutcome.END_OF_SHARD) {
            markForShutdown(ShutdownReason.TERMINATE);
            LOG.info("Shard " + shardInfo.getShardId() + ": Mark for shutdown with reason TERMINATE");
        }
        if (taskOutcome == TaskOutcome.LEASE_NOT_FOUND) {
            markForShutdown(ShutdownReason.ZOMBIE);
            LOG.info("Shard " + shardInfo.getShardId() + ": Mark for shutdown with reason ZOMBIE as lease was not found");
        }
        if (isShutdownRequested() && taskOutcome != TaskOutcome.FAILURE) {
            currentState = currentState.shutdownTransition(shutdownReason);
        } else if (isShutdownRequested() && DynamoDBStreamsConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS.equals(currentState.getState())) {
            currentState = currentState.shutdownTransition(shutdownReason);
        } else if (taskOutcome == TaskOutcome.SUCCESSFUL) {
            if (currentState.getTaskType() == currentTask.getTaskType()) {
                currentState = currentState.successTransition();
            } else {
                LOG.error("Current State task type of '" + currentState.getTaskType()
                        + "' doesn't match the current tasks type of '" + currentTask.getTaskType()
                        + "'.  This shouldn't happen, and indicates a programming error. "
                        + "Unable to safely transition to the next state.");
            }
        }
        //
        // Don't change state otherwise
        //

    }

    @VisibleForTesting
    public boolean isShutdownRequested() {
        return shutdownReason != null;

    }

    /**
     * Private/Internal method - has package level access solely for testing purposes.
     *
     * @return the currentState
     */
    public KinesisConsumerStates.ShardConsumerState getCurrentState() { //getCurrentState required in interface but only used in comparison in worker, so quick ddb to kinesis state translation is used;
        switch(currentState.getState()){

            case WAITING_ON_PARENT_SHARDS:
                return KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS;

            case INITIALIZING:
                return KinesisConsumerStates.ShardConsumerState.INITIALIZING;

            case PROCESSING:
                return KinesisConsumerStates.ShardConsumerState.PROCESSING;

            case SHUTDOWN_REQUESTED:
                return KinesisConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED;

            case SHUTTING_DOWN:
                return KinesisConsumerStates.ShardConsumerState.SHUTTING_DOWN;

            case SHUTDOWN_COMPLETE:
                return KinesisConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE;

            default:
                return null; //if incorrect state is inputted
        }
    }

    StreamConfig getStreamConfig() {
        return streamConfig;
    }

    IRecordProcessor getRecordProcessor() {
        return recordProcessor;
    }

    RecordProcessorCheckpointer getRecordProcessorCheckpointer() {
        return recordProcessorCheckpointer;
    }

    ExecutorService getExecutorService() {
        return executorService;
    }

    ShardInfo getShardInfo() {
        return shardInfo;
    }

    DynamoDBStreamsDataFetcher getDataFetcher() {
        return dataFetcher;
    }

    ILeaseManager<KinesisClientLease> getLeaseManager() {
        return leaseCoordinator.getLeaseManager();
    }

    KinesisClientLibLeaseCoordinator getLeaseCoordinator() {
        return leaseCoordinator;
    }

    ICheckpoint getCheckpoint() {
        return checkpoint;
    }

    long getParentShardPollIntervalMillis() {
        return parentShardPollIntervalMillis;
    }

    boolean isCleanupLeasesOfCompletedShards() {
        return cleanupLeasesOfCompletedShards;
    }

    boolean isIgnoreUnexpectedChildShards() {
        return config.shouldIgnoreUnexpectedChildShards();
    }

    long getTaskBackoffTimeMillis() {
        return taskBackoffTimeMillis;
    }

    Future<TaskResult> getFuture() {
        return future;
    }

    ShutdownNotification getShutdownNotification() {
        return shutdownNotification;
    }

    ShardSyncStrategy getShardSyncStrategy() {
        return shardSyncStrategy;
    }

    LeaseCleanupManager getLeaseCleanupManager() {
        return leaseCleanupManager;
    }
}
