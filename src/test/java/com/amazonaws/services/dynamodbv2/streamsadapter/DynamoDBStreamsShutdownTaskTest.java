package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.streamsadapter.util.ShardObjectHelper;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
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
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBStreamsShutdownTaskTest {
    private static final long TASK_BACKOFF_TIME_MILLIS = 1L;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_TRIM_HORIZON =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

    Set<String> defaultParentShardIds = new HashSet<>();
    String defaultConcurrencyToken = UUID.randomUUID().toString();
    String defaultShardId = "shardId-0";
    ShardInfo defaultShardInfo = new ShardInfo(defaultShardId,
            defaultConcurrencyToken,
            defaultParentShardIds,
            ExtendedSequenceNumber.LATEST);
    ShardSyncer shardSyncer = new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
    IMetricsFactory metricsFactory = new NullMetricsFactory();


    @Mock
    private IKinesisProxy kinesisProxy;
    @Mock
    private GetRecordsCache getRecordsCache;
    @Mock
    private ShardSyncStrategy shardSyncStrategy;
    @Mock
    private KinesisClientLibLeaseCoordinator leaseCoordinator;
    @Mock
    private IRecordProcessor defaultRecordProcessor;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        doNothing().when(getRecordsCache).shutdown();
        final KinesisClientLease parentLease = createLease(defaultShardId, "leaseOwner", Collections.emptyList());
        parentLease.setCheckpoint(new ExtendedSequenceNumber("3298"));
        when(leaseCoordinator.getCurrentlyHeldLease(defaultShardId)).thenReturn(parentLease);
        when(kinesisProxy.verifyShardClosure(anyString())).thenReturn(() -> true);
        when(shardSyncStrategy.onShardConsumerShutDown(any())).thenReturn(new TaskResult(null));
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link DynamoDBStreamsShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenApplicationDoesNotCheckpoint() {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;
        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy);
        TaskResult result = task.call();
        assertNotNull(result.getException());
        assertTrue(result.getException() instanceof IllegalArgumentException);
        final String expectedExceptionMessage = "Application didn't checkpoint at end of shard shardId-0. Application must checkpoint upon shutdown. See IRecordProcessor.shutdown javadocs for more information.";
        Assert.assertEquals(expectedExceptionMessage, result.getException().getMessage());
    }

    /**
     * Test method for {@link DynamoDBStreamsShutdownTask#call()}.
     */
    @Test
    public final void testCallWhenCreatingLeaseThrows() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy);
        TaskResult result = task.call();
        verify(getRecordsCache).shutdown();
        assertNull(result.getException());
    }

    @Test
    public final void testCallWhenShardEnd() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        final KinesisClientLease parentLease1 = createLease(defaultShardId, "leaseOwner", Collections.emptyList());
        parentLease1.setCheckpoint(new ExtendedSequenceNumber("3298"));
        final KinesisClientLease parentLease2 = createLease(defaultShardId, "leaseOwner", Collections.emptyList());
        parentLease2.setCheckpoint(ExtendedSequenceNumber.SHARD_END);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy);
        TaskResult result = task.call();
        assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    @Test
    public final void testCallWhenShardNotFound() throws Exception {
        ShardInfo shardInfo = new ShardInfo("shardId-4",
                defaultConcurrencyToken,
                defaultParentShardIds,
                ExtendedSequenceNumber.LATEST);
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(shardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.TERMINATE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy);
        TaskResult result = task.call();
        assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    @Test
    public final void testCallWhenLeaseLost() throws Exception {
        RecordProcessorCheckpointer checkpointer = mock(RecordProcessorCheckpointer.class);
        when(checkpointer.getLastCheckpointValue()).thenReturn(new ExtendedSequenceNumber("3298"));
        boolean cleanupLeasesOfCompletedShards = false;
        boolean ignoreUnexpectedChildShards = false;

        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(defaultShardInfo,
                defaultRecordProcessor,
                checkpointer,
                ShutdownReason.ZOMBIE,
                kinesisProxy,
                INITIAL_POSITION_TRIM_HORIZON,
                cleanupLeasesOfCompletedShards,
                ignoreUnexpectedChildShards,
                leaseCoordinator,
                TASK_BACKOFF_TIME_MILLIS,
                getRecordsCache,
                shardSyncer,
                shardSyncStrategy);
        TaskResult result = task.call();
        assertNull(result.getException());
        verify(getRecordsCache).shutdown();
    }

    /**
     * Test method for {@link DynamoDBStreamsShutdownTask#getTaskType()}.
     */
    @Test
    public final void testGetTaskType() {
        KinesisClientLibLeaseCoordinator leaseCoordinator = mock(KinesisClientLibLeaseCoordinator.class);
        DynamoDBStreamsShutdownTask task = new DynamoDBStreamsShutdownTask(null, null, null, null,
                null, null, false,
                false, leaseCoordinator, 0,
                getRecordsCache, shardSyncer, shardSyncStrategy);
        Assert.assertEquals(TaskType.SHUTDOWN, task.getTaskType());
    }



    private KinesisClientLease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds) {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setLeaseKey(leaseKey);
        lease.setLeaseOwner(leaseOwner);
        lease.setParentShardIds(parentShardIds);
        return lease;
    }

    /*
     * Helper method to construct a shard list for graph A. Graph A is defined below.
     * Shard structure (y-axis is epochs):
     * 0 1 2 3 4   5- shards till epoch 102
     * \ / \ / |  |
     *  6   7  4   5- shards from epoch 103 - 205
     *   \ /   |  /\
     *    8    4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
     */
    private List<Shard> constructShardListForGraphA() {
        List<Shard> shards = new ArrayList<Shard>();

        SequenceNumberRange range0 = ShardObjectHelper.newSequenceNumberRange("11", "102");
        SequenceNumberRange range1 = ShardObjectHelper.newSequenceNumberRange("11", null);
        SequenceNumberRange range2 = ShardObjectHelper.newSequenceNumberRange("11", "210");
        SequenceNumberRange range3 = ShardObjectHelper.newSequenceNumberRange("103", "210");
        SequenceNumberRange range4 = ShardObjectHelper.newSequenceNumberRange("211", null);

        HashKeyRange hashRange0 = ShardObjectHelper.newHashKeyRange("0", "99");
        HashKeyRange hashRange1 = ShardObjectHelper.newHashKeyRange("100", "199");
        HashKeyRange hashRange2 = ShardObjectHelper.newHashKeyRange("200", "299");
        HashKeyRange hashRange3 = ShardObjectHelper.newHashKeyRange("300", "399");
        HashKeyRange hashRange4 = ShardObjectHelper.newHashKeyRange("400", "499");
        HashKeyRange hashRange5 = ShardObjectHelper.newHashKeyRange("500", ShardObjectHelper.MAX_HASH_KEY);
        HashKeyRange hashRange6 = ShardObjectHelper.newHashKeyRange("0", "199");
        HashKeyRange hashRange7 = ShardObjectHelper.newHashKeyRange("200", "399");
        HashKeyRange hashRange8 = ShardObjectHelper.newHashKeyRange("0", "399");
        HashKeyRange hashRange9 = ShardObjectHelper.newHashKeyRange("500", "799");
        HashKeyRange hashRange10 = ShardObjectHelper.newHashKeyRange("800", ShardObjectHelper.MAX_HASH_KEY);

        shards.add(ShardObjectHelper.newShard("shardId-0", null, null, range0, hashRange0));
        shards.add(ShardObjectHelper.newShard("shardId-1", null, null, range0, hashRange1));
        shards.add(ShardObjectHelper.newShard("shardId-2", null, null, range0, hashRange2));
        shards.add(ShardObjectHelper.newShard("shardId-3", null, null, range0, hashRange3));
        shards.add(ShardObjectHelper.newShard("shardId-4", null, null, range1, hashRange4));
        shards.add(ShardObjectHelper.newShard("shardId-5", null, null, range2, hashRange5));

        shards.add(ShardObjectHelper.newShard("shardId-6", "shardId-0", "shardId-1", range3, hashRange6));
        shards.add(ShardObjectHelper.newShard("shardId-7", "shardId-2", "shardId-3", range3, hashRange7));

        shards.add(ShardObjectHelper.newShard("shardId-8", "shardId-6", "shardId-7", range4, hashRange8));
        shards.add(ShardObjectHelper.newShard("shardId-9", "shardId-5", null, range4, hashRange9));
        shards.add(ShardObjectHelper.newShard("shardId-10", null, "shardId-5", range4, hashRange10));

        return shards;
    }
}
