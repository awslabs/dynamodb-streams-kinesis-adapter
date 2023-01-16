package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.*;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.MetricsHelper;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.util.CollectionUtils;
import com.google.common.annotations.VisibleForTesting;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@Getter
@EqualsAndHashCode
public class DynamoDBStreamsPeriodicShardSyncManager implements IPeriodicShardSyncManager {

    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsPeriodicShardSyncManager.class);
    private static final long INITIAL_DELAY = 0;

    /** DEFAULT interval is used for PERIODIC {@link ShardSyncStrategyType}. */

    private static final long DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS = 1000L;

    /** Parameters for validating hash range completeness when running in auditor mode. */
    @VisibleForTesting
    static final BigInteger MIN_HASH_KEY = BigInteger.ZERO;
    @VisibleForTesting
    static final BigInteger MAX_HASH_KEY = new BigInteger("2").pow(128).subtract(BigInteger.ONE);
    static final String PERIODIC_SHARD_SYNC_MANAGER = "PeriodicShardSyncManager";
    private final String workerId;
    private final LeaderDecider leaderDecider;
    private final ITask metricsEmittingShardSyncTask;
    private final ScheduledExecutorService shardSyncThreadPool;
    private final ILeaseManager<KinesisClientLease> leaseManager;
    private final IKinesisProxy kinesisProxy;
    private final boolean isAuditorMode;
    private final long periodicShardSyncIntervalMillis;
    private boolean isRunning;
    private final IMetricsFactory metricsFactory;
    private final int leasesRecoveryAuditorInconsistencyConfidenceThreshold;

    DynamoDBStreamsPeriodicShardSyncManager(String workerId,
                                            LeaderDecider leaderDecider,
                                            ShardSyncTask shardSyncTask,
                                            IMetricsFactory metricsFactory,
                                            ILeaseManager<KinesisClientLease> leaseManager,
                                            IKinesisProxy kinesisProxy,
                                            boolean isAuditorMode,
                                            long leasesRecoveryAuditorExecutionFrequencyMillis,
                                            int leasesRecoveryAuditorInconsistencyConfidenceThreshold) {
        this(workerId, leaderDecider, shardSyncTask, Executors.newSingleThreadScheduledExecutor(), metricsFactory,
                leaseManager, kinesisProxy, isAuditorMode, leasesRecoveryAuditorExecutionFrequencyMillis,
                leasesRecoveryAuditorInconsistencyConfidenceThreshold);
    }

    DynamoDBStreamsPeriodicShardSyncManager(String workerId,
                                            LeaderDecider leaderDecider,
                                            ShardSyncTask shardSyncTask,
                                            ScheduledExecutorService shardSyncThreadPool,
                                            IMetricsFactory metricsFactory,
                                            ILeaseManager<KinesisClientLease> leaseManager,
                                            IKinesisProxy kinesisProxy,
                                            boolean isAuditorMode,
                                            long leasesRecoveryAuditorExecutionFrequencyMillis,
                                            int leasesRecoveryAuditorInconsistencyConfidenceThreshold) {
        Validate.notBlank(workerId, "WorkerID is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(leaderDecider, "LeaderDecider is required to initialize PeriodicShardSyncManager.");
        Validate.notNull(shardSyncTask, "ShardSyncTask is required to initialize PeriodicShardSyncManager.");
        this.workerId = workerId;
        this.leaderDecider = leaderDecider;
        this.metricsEmittingShardSyncTask = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory);
        this.shardSyncThreadPool = shardSyncThreadPool;
        this.leaseManager = leaseManager;
        this.kinesisProxy = kinesisProxy;
        this.metricsFactory = metricsFactory;
        this.isAuditorMode = isAuditorMode;
        this.leasesRecoveryAuditorInconsistencyConfidenceThreshold = leasesRecoveryAuditorInconsistencyConfidenceThreshold;
        if (isAuditorMode) {
            Validate.notNull(this.leaseManager, "LeaseManager is required for non-PERIODIC shard sync strategies.");
            Validate.notNull(this.kinesisProxy, "KinesisProxy is required for non-PERIODIC shard sync strategies.");
            this.periodicShardSyncIntervalMillis = leasesRecoveryAuditorExecutionFrequencyMillis;
        } else {
            this.periodicShardSyncIntervalMillis = DEFAULT_PERIODIC_SHARD_SYNC_INTERVAL_MILLIS;
        }
    }

    @Override
    public synchronized TaskResult start() {
        if (!isRunning) {
            final Runnable periodicShardSyncer = () -> {
                try {
                    runShardSync();
                } catch (Throwable t) {
                    LOG.error("Error running shard sync.", t);
                }
            };

            shardSyncThreadPool
                    .scheduleWithFixedDelay(periodicShardSyncer, INITIAL_DELAY, periodicShardSyncIntervalMillis,
                            TimeUnit.MILLISECONDS);
            isRunning = true;
        }
        return new TaskResult(null);
    }

    /**
     * Runs ShardSync once, without scheduling further periodic ShardSyncs.
     * @return TaskResult from shard sync
     */
    @Override
    public synchronized TaskResult syncShardsOnce() {
        LOG.info("Syncing shards once from worker " + workerId);
        return metricsEmittingShardSyncTask.call();
    }

    @Override
    public void stop() {
        if (isRunning) {
            LOG.info(String.format("Shutting down leader decider on worker %s", workerId));
            leaderDecider.shutdown();
            LOG.info(String.format("Shutting down periodic shard sync task scheduler on worker %s", workerId));
            shardSyncThreadPool.shutdown();
            isRunning = false;
        }
    }

    private void runShardSync() {
        if (leaderDecider.isLeader(workerId)) {
            LOG.debug("WorkerId " + workerId + " is a leader, running the shard sync task");

            MetricsHelper.startScope(metricsFactory, PERIODIC_SHARD_SYNC_MANAGER);
            boolean isRunSuccess = false;
            final long runStartMillis = System.currentTimeMillis();

            try {
                final ShardSyncResponse shardSyncResponse = checkForShardSync();
                MetricsHelper.getMetricsScope().addData("NumStreamsToSync", shardSyncResponse.shouldDoShardSync() ? 1 : 0, StandardUnit.Count, MetricsLevel.SUMMARY);
                if (shardSyncResponse.shouldDoShardSync()) {
                    LOG.info("Periodic shard syncer initiating shard sync due to the reason - " +
                            shardSyncResponse.reasonForDecision());
                    metricsEmittingShardSyncTask.call();
                } else {
                    LOG.info("Skipping shard sync due to the reason - " + shardSyncResponse.reasonForDecision());
                }
                isRunSuccess = true;
            } catch (Exception e) {
                LOG.error("Caught exception while running periodic shard syncer.", e);
            } finally {
                MetricsHelper.addSuccessAndLatency(runStartMillis, isRunSuccess, MetricsLevel.SUMMARY);
                MetricsHelper.endScope();
            }
        } else {
            LOG.debug("WorkerId " + workerId + " is not a leader, not running the shard sync task");
        }
    }

    @VisibleForTesting
    ShardSyncResponse checkForShardSync() throws DependencyException, InvalidStateException,
            ProvisionedThroughputException {

        if (!isAuditorMode) {
            // If we are running with PERIODIC shard sync strategy, we should sync every time.
            return new ShardSyncResponse(true, false, "Syncing every time with PERIODIC shard sync strategy.");
        }

        // Get current leases from DynamoDB.
        final List<KinesisClientLease> currentLeases = leaseManager.listLeases();

        if (CollectionUtils.isNullOrEmpty(currentLeases)) {
            // If the current leases are null or empty, then we need to initiate a shard sync.
            LOG.info("No leases found. Will trigger a shard sync.");
            return new ShardSyncResponse(true, false, "No leases found.");
        }

        //Return Default Response indicating DDB Streams doesn't do hash range hole checking
        return new ShardSyncResponse(false, false, "DynamoDB Streams does not support hash range hole checking");
    }
}
