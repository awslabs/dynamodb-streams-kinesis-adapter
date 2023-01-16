package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.IPeriodicShardSyncManager;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.LeaderDecider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncTask;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBStreamsPeriodicShardSyncManagerTest {
    private static final String WORKER_ID = "workerId";
    public static final long LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS = 2 * 60 * 1000L;
    public static final int LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD = 3;

    /** Manager for PERIODIC shard sync strategy */
    private IPeriodicShardSyncManager periodicShardSyncManager;

    /** Manager for SHARD_END shard sync strategy */
    private IPeriodicShardSyncManager auditorPeriodicShardSyncManager;

    @Mock
    private LeaderDecider leaderDecider;
    @Mock
    private ShardSyncTask shardSyncTask;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private IKinesisProxy kinesisProxy;

    private IMetricsFactory metricsFactory = new NullMetricsFactory();

    @Before
    public void setup() {
        periodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(WORKER_ID, leaderDecider, shardSyncTask,
                metricsFactory, leaseManager, kinesisProxy, false, LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS,
                LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD);
        auditorPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(WORKER_ID, leaderDecider, shardSyncTask,
                metricsFactory, leaseManager, kinesisProxy, true, LEASES_RECOVERY_AUDITOR_EXECUTION_FREQUENCY_MILLIS,
                LEASES_RECOVERY_AUDITOR_INCONSISTENCY_CONFIDENCE_THRESHOLD);
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenNoLeasesArePassed() throws Exception {
        when(leaseManager.listLeases()).thenReturn(null);
        Assert.assertTrue(((DynamoDBStreamsPeriodicShardSyncManager) periodicShardSyncManager).checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(((DynamoDBStreamsPeriodicShardSyncManager) auditorPeriodicShardSyncManager).checkForShardSync().shouldDoShardSync());
    }

    @Test
    public void testIfShardSyncIsInitiatedWhenEmptyLeasesArePassed() throws Exception {
        when(leaseManager.listLeases()).thenReturn(Collections.emptyList());
        Assert.assertTrue(((DynamoDBStreamsPeriodicShardSyncManager) periodicShardSyncManager).checkForShardSync().shouldDoShardSync());
        Assert.assertTrue(((DynamoDBStreamsPeriodicShardSyncManager) auditorPeriodicShardSyncManager).checkForShardSync().shouldDoShardSync());
    }

}
