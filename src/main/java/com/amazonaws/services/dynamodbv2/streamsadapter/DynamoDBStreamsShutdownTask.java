package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibLeaseCoordinator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.TaskResult;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.TaskType;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.proxies.ShardClosureVerificationResponse;
import com.amazonaws.services.kinesis.clientlibrary.proxies.ShardListWrappingShardClosureVerificationResponse;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Shard;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class DynamoDBStreamsShutdownTask implements ITask {

    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsShutdownTask.class);

    private static final String RECORD_PROCESSOR_SHUTDOWN_METRIC = "RecordProcessor.shutdown";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    private final ShutdownReason reason;
    private final IKinesisProxy kinesisProxy;
    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final InitialPositionInStreamExtended initialPositionInStream;
    private final boolean cleanupLeasesOfCompletedShards;
    private final boolean ignoreUnexpectedChildShards;
    private final TaskType taskType = TaskType.SHUTDOWN;
    private final long backoffTimeMillis;
    private final GetRecordsCache getRecordsCache;
    private final ShardSyncer shardSyncer;
    private final ShardSyncStrategy shardSyncStrategy;

    /**
     * Constructor.
     */
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    DynamoDBStreamsShutdownTask(ShardInfo shardInfo,
            IRecordProcessor recordProcessor,
            RecordProcessorCheckpointer recordProcessorCheckpointer,
            ShutdownReason reason,
            IKinesisProxy kinesisProxy,
            InitialPositionInStreamExtended initialPositionInStream,
            boolean cleanupLeasesOfCompletedShards,
            boolean ignoreUnexpectedChildShards,
            KinesisClientLibLeaseCoordinator leaseCoordinator,
            long backoffTimeMillis,
            GetRecordsCache getRecordsCache, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.reason = reason;
        this.kinesisProxy = kinesisProxy;
        this.initialPositionInStream = initialPositionInStream;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.ignoreUnexpectedChildShards = ignoreUnexpectedChildShards;
        this.leaseCoordinator = leaseCoordinator;
        this.backoffTimeMillis = backoffTimeMillis;
        this.getRecordsCache = getRecordsCache;
        this.shardSyncer = shardSyncer;
        this.shardSyncStrategy = shardSyncStrategy;
    }

    /*
     * Invokes RecordProcessor shutdown() API.
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#call()
     */
    @Override
    public TaskResult call() {
        Exception exception;
        boolean applicationException = false;

        try {
            ShutdownReason localReason = reason;
            List<Shard> latestShards = null;
            /*
             * Revalidate if the current shard is closed before shutting down the shard consumer with reason SHARD_END
             * If current shard is not closed, shut down the shard consumer with reason LEASE_LOST that allows active
             * workers to contend for the lease of this shard.
             */
            if(localReason == ShutdownReason.TERMINATE) {
                ShardClosureVerificationResponse shardClosureVerificationResponse = kinesisProxy.verifyShardClosure(shardInfo.getShardId());
                if (shardClosureVerificationResponse instanceof ShardListWrappingShardClosureVerificationResponse) {
                    latestShards = ((ShardListWrappingShardClosureVerificationResponse)shardClosureVerificationResponse).getLatestShards();
                }

                // If shard in context is not closed yet we should shut down the ShardConsumer with Zombie state
                // which avoids checkpoint-ing with SHARD_END sequence number.
                if(!shardClosureVerificationResponse.isShardClosed()) {
                    localReason = ShutdownReason.ZOMBIE;
                    dropLease();
                    LOG.info("Forcing the lease to be lost before shutting down the consumer for Shard: " + shardInfo.getShardId());
                }
            }


            // If we reached end of the shard, set sequence number to SHARD_END.
            if (localReason == ShutdownReason.TERMINATE) {
                recordProcessorCheckpointer.setSequenceNumberAtShardEnd(
                        recordProcessorCheckpointer.getLargestPermittedCheckpointValue());
                recordProcessorCheckpointer.setLargestPermittedCheckpointValue(ExtendedSequenceNumber.SHARD_END);
            }

            LOG.debug("Invoking shutdown() for shard " + shardInfo.getShardId() + ", concurrencyToken "
                    + shardInfo.getConcurrencyToken() + ". Shutdown reason: " + localReason);
            final ShutdownInput shutdownInput = new ShutdownInput()
                    .withShutdownReason(localReason)
                    .withCheckpointer(recordProcessorCheckpointer);
            final long recordProcessorStartTimeMillis = System.currentTimeMillis();
            try {
                recordProcessor.shutdown(shutdownInput);
                ExtendedSequenceNumber lastCheckpointValue = recordProcessorCheckpointer.getLastCheckpointValue();

                if (localReason == ShutdownReason.TERMINATE) {
                    if ((lastCheckpointValue == null)
                            || (!lastCheckpointValue.equals(ExtendedSequenceNumber.SHARD_END))) {
                        throw new IllegalArgumentException("Application didn't checkpoint at end of shard "
                                + shardInfo.getShardId() + ". Application must checkpoint upon shutdown. " +
                                "See IRecordProcessor.shutdown javadocs for more information.");
                    }
                }
                LOG.debug("Shutting down retrieval strategy.");
                getRecordsCache.shutdown();
                LOG.debug("Record processor completed shutdown() for shard " + shardInfo.getShardId());
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsHelper.addLatency(RECORD_PROCESSOR_SHUTDOWN_METRIC, recordProcessorStartTimeMillis,
                        MetricsLevel.SUMMARY);
            }

            if (localReason == ShutdownReason.TERMINATE) {
                LOG.debug("Looking for child shards of shard " + shardInfo.getShardId());
                // create leases for the child shards
                TaskResult result = shardSyncStrategy.onShardConsumerShutDown(latestShards);
                if (result.getException() != null) {
                    LOG.debug("Exception while trying to sync shards on the shutdown of shard: " + shardInfo
                            .getShardId());
                    throw result.getException();
                }
                LOG.debug("Finished checking for child shards of shard " + shardInfo.getShardId());
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                LOG.error("Application exception. ", e);
            } else {
                LOG.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
        }

        return new TaskResult(exception);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

    @VisibleForTesting
    ShutdownReason getReason() {
        return reason;
    }

    private void dropLease() {
        KinesisClientLease lease = leaseCoordinator.getCurrentlyHeldLease(shardInfo.getShardId());
        leaseCoordinator.dropLease(lease);
        LOG.warn("Dropped lease for shutting down ShardConsumer: " + lease.getLeaseKey());
    }
}