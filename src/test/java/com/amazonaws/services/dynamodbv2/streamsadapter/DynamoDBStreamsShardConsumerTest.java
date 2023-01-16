package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisLocalFileDataCreator;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisLocalFileProxy;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.Checkpoint;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.AsynchronousGetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.BlockingGetRecordsCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsCache;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.GetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ITask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStreamExtended;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitializeTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibLeaseCoordinator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisConsumerStates;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordsFetcherFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SequenceNumberValidator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownNotification;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.StreamConfig;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SynchronousGetRecordsRetrievalStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.TaskResult;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)
public class DynamoDBStreamsShardConsumerTest {
    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsShardConsumerTest.class);

    private final IMetricsFactory metricsFactory = new NullMetricsFactory();
    private final boolean callProcessRecordsForEmptyRecordList = false;
    private final long taskBackoffTimeMillis = 500L;
    private final long parentShardPollIntervalMillis = 50L;
    private final boolean cleanupLeasesOfCompletedShards = true;
    // We don't want any of these tests to run checkpoint validation
    private final boolean skipCheckpointValidationValue = false;
    private static final InitialPositionInStreamExtended INITIAL_POSITION_LATEST =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
    private static final ShardSyncer shardSyncer = new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());

    // Use Executors.newFixedThreadPool since it returns ThreadPoolExecutor, which is
    // ... a non-final public class, and so can be mocked and spied.
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private RecordsFetcherFactory recordsFetcherFactory;

    private GetRecordsCache getRecordsCache;

    private DynamoDBStreamsDataFetcher dataFetcher;

    @Mock
    private IRecordProcessor processor;
    @Mock
    private KinesisClientLibConfiguration config;
    @Mock
    private IKinesisProxy streamProxy;
    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;
    @Mock
    private KinesisClientLibLeaseCoordinator leaseCoordinator;
    @Mock
    private ICheckpoint checkpoint;
    @Mock
    private ShutdownNotification shutdownNotification;
    @Mock
    ShardSyncStrategy shardSyncStrategy;

    @Mock
    private StreamConfig streamConfig;

    @Before
    public void setup() {
        getRecordsCache = null;
        dataFetcher = null;

        recordsFetcherFactory = spy(new SimpleRecordsFetcherFactory());
        when(config.getRecordsFetcherFactory()).thenReturn(recordsFetcherFactory);
        when(config.getLogWarningForTaskAfterMillis()).thenReturn(Optional.empty());
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(streamConfig.getStreamProxy()).thenReturn(streamProxy);
        when(streamConfig.getInitialPositionInStream()).thenReturn(INITIAL_POSITION_LATEST);
        when(streamConfig.shouldValidateSequenceNumberBeforeCheckpointing()).thenReturn(skipCheckpointValidationValue);
    }

    /**
     * Test method to verify consumer stays in INITIALIZING state when InitializationTask fails.
     */
    @SuppressWarnings("unchecked")
    @Test
    public final void testInitializationStateUponSubmissionFailure() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);
        ExecutorService spyExecutorService = spy(executorService);

        when(checkpoint.getCheckpoint(anyString())).thenThrow(NullPointerException.class);
        when(checkpoint.getCheckpointObject(anyString())).thenThrow(NullPointerException.class);
        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);

        DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        spyExecutorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));

        doThrow(new RejectedExecutionException()).when(spyExecutorService).submit(any(InitializeTask.class));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
    }

    @Test
    public void testInitializationStateTransitionsToShutdownOnLeaseNotFound() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);

        ICheckpoint checkpoint = new KinesisClientLibLeaseCoordinator(leaseManager, "", 0, 0);

        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);

        DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        config,
                        shardSyncer,
                        shardSyncStrategy);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTTING_DOWN)));
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE)));
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testRecordProcessorThrowable() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", UUID.randomUUID().toString(), null, ExtendedSequenceNumber.TRIM_HORIZON);

        DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        final ExtendedSequenceNumber checkpointSequenceNumber = new ExtendedSequenceNumber("123");
        final ExtendedSequenceNumber pendingCheckpointSequenceNumber = null;
        when(streamProxy.getIterator(anyString(), anyString(), anyString())).thenReturn("startingIterator");
        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(
                new Checkpoint(checkpointSequenceNumber, pendingCheckpointSequenceNumber));

        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // submit BlockOnParentShardTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        verify(processor, times(0)).initialize(any(InitializationInput.class));

        // Throw Error when IRecordProcessor.initialize() is invoked.
        doThrow(new Error("ThrowableTest")).when(processor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(processor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));

        try {
            // Checking the status of submitted InitializeTask from above should throw exception.
            consumer.consumeShard();
            fail("ShardConsumer should have thrown exception.");
        } catch (RuntimeException e) {
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
        }
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(processor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));

        doNothing().when(processor).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask again.
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(processor, times(2)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));
        verify(processor, times(2)).initialize(any(InitializationInput.class)); // no other calls with different args

        // Checking the status of submitted InitializeTask from above should pass.
        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.PROCESSING)));
    }

    /**
     * Test method for {@link KinesisShardConsumer#consumeShard()}
     */
    @Test
    public final void testConsumeShard() throws Exception {
        int numRecs = 10;
        BigInteger startSeqNum = BigInteger.ONE;
        String streamShardId = "kinesis-0-0";
        String testConcurrencyToken = "testToken";
        File file =
                KinesisLocalFileDataCreator.generateTempDataFile(1,
                        "kinesis-0-",
                        numRecs,
                        startSeqNum,
                        "unitTestSCT001");

        IKinesisProxy fileBasedProxy = new KinesisLocalFileProxy(file.getAbsolutePath());

        final int maxRecords = 2;
        ICheckpoint checkpoint = mock(ICheckpoint.class);
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(new Checkpoint(new ExtendedSequenceNumber(startSeqNum.toString()), ExtendedSequenceNumber.TRIM_HORIZON));
        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(streamConfig.getStreamProxy()).thenReturn(fileBasedProxy);


        ShardInfo shardInfo = new ShardInfo(streamShardId, testConcurrencyToken, null, null);

        RecordProcessorCheckpointer recordProcessorCheckpointer = new RecordProcessorCheckpointer(
                shardInfo,
                checkpoint,
                new SequenceNumberValidator(
                        streamConfig.getStreamProxy(),
                        shardInfo.getShardId(),
                        streamConfig.shouldValidateSequenceNumberBeforeCheckpointing()
                ),
                metricsFactory
        );

        dataFetcher = new DynamoDBStreamsDataFetcher(streamConfig.getStreamProxy(), shardInfo);

        getRecordsCache = spy(new BlockingGetRecordsCache(maxRecords,
                new SynchronousGetRecordsRetrievalStrategy(dataFetcher)));
        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt()))
                .thenReturn(getRecordsCache);

        DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        recordProcessorCheckpointer,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        dataFetcher,
                        Optional.empty(),
                        Optional.empty(),
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(50L);
        consumer.consumeShard(); // start initialization
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        consumer.consumeShard(); // initialize
        Thread.sleep(50L);
        // We expect to process all records in numRecs calls
        for (int i = 0; i < numRecs;) {
            boolean newTaskSubmitted = consumer.consumeShard();
            if (newTaskSubmitted) {
                LOG.debug("New processing task was submitted, call # " + i);
                assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.PROCESSING)));
                // CHECKSTYLE:IGNORE ModifiedControlVariable FOR NEXT 1 LINES
                i += maxRecords;
            }
            Thread.sleep(50L);
        }

        verify(getRecordsCache, times(5)).getNextResult();

        assertThat(consumer.getShutdownReason(), nullValue());
        consumer.notifyShutdownRequested(shutdownNotification);
        consumer.consumeShard();
        Thread.sleep(50);
        assertThat(consumer.getShutdownReason(), equalTo(ShutdownReason.REQUESTED));
        assertThat(consumer.getCurrentState(), equalTo(KinesisConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED));
        verify(shutdownNotification).shutdownNotificationComplete();
        consumer.consumeShard();
        Thread.sleep(50);
        assertThat(consumer.getCurrentState(), equalTo(KinesisConsumerStates.ShardConsumerState.SHUTDOWN_REQUESTED));

        consumer.beginShutdown();
        Thread.sleep(50L);
        assertThat(consumer.getShutdownReason(), equalTo(ShutdownReason.ZOMBIE));
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTTING_DOWN)));
        consumer.beginShutdown();
        consumer.consumeShard();
        verify(shutdownNotification, atLeastOnce()).shutdownComplete();
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(consumer.getShutdownReason(), is(equalTo(ShutdownReason.ZOMBIE)));

        verify(getRecordsCache).shutdown();

        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        String iterator = fileBasedProxy.getIterator(streamShardId, ShardIteratorType.TRIM_HORIZON.toString());
        file.delete();
    }

    private String randomShardId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Test that a consumer can be shut down while it is blocked on parent
     */
    @Test
    public final void testShardConsumerShutdownWhenBlockedOnParent() throws Exception {
        final StreamConfig streamConfig = mock(StreamConfig.class);
        final RecordProcessorCheckpointer recordProcessorCheckpointer = mock(RecordProcessorCheckpointer.class);
        final GetRecordsCache getRecordsCache = mock(GetRecordsCache.class);
        final DynamoDBStreamsDataFetcher dataFetcher = mock(DynamoDBStreamsDataFetcher.class);
        when(recordsFetcherFactory.createRecordsFetcher(any(GetRecordsRetrievalStrategy.class), anyString(),
                any(IMetricsFactory.class), anyInt())).thenReturn(getRecordsCache);
        final String shardId = randomShardId();
        final String parentShardId = randomShardId();
        final ShardInfo shardInfo = mock(ShardInfo.class);
        final KinesisClientLease parentLease = mock(KinesisClientLease.class);
        when(shardInfo.getShardId()).thenReturn(shardId);
        when(shardInfo.getParentShardIds()).thenReturn(Arrays.asList(parentShardId));
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(leaseManager.getLease(eq(parentShardId))).thenReturn(parentLease);
        when(parentLease.getCheckpoint()).thenReturn(ExtendedSequenceNumber.TRIM_HORIZON);
        when(recordProcessorCheckpointer.getLastCheckpointValue()).thenReturn(ExtendedSequenceNumber.SHARD_END);
        when(streamConfig.getStreamProxy()).thenReturn(streamProxy);

        final DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        recordProcessorCheckpointer,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        dataFetcher,
                        Optional.empty(),
                        Optional.empty(),
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        verify(parentLease, times(0)).getCheckpoint();
        consumer.consumeShard(); // check on parent shards
        Thread.sleep(parentShardPollIntervalMillis * 2);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        verify(parentLease, times(1)).getCheckpoint();
        consumer.notifyShutdownRequested(shutdownNotification);
        verify(shutdownNotification, times(0)).shutdownComplete();
        assertThat(consumer.getShutdownReason(), equalTo(ShutdownReason.REQUESTED));
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard();
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTTING_DOWN)));
        Thread.sleep(50L);
        consumer.beginShutdown();
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.SHUTDOWN_COMPLETE)));
        assertThat(consumer.isShutdown(), is(true));
        verify(shutdownNotification, times(1)).shutdownComplete();
        consumer.beginShutdown();
        assertThat(consumer.getShutdownReason(), equalTo(ShutdownReason.ZOMBIE));
        assertThat(consumer.isShutdown(), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public final void testConsumeShardInitializedWithPendingCheckpoint() throws Exception {
        ShardInfo shardInfo = new ShardInfo("s-0-0", UUID.randomUUID().toString(), null, ExtendedSequenceNumber.TRIM_HORIZON);

        DynamoDBStreamsShardConsumer consumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        GetRecordsCache getRecordsCache = spy(consumer.getGetRecordsCache());

        final ExtendedSequenceNumber checkpointSequenceNumber = new ExtendedSequenceNumber("123");
        final ExtendedSequenceNumber pendingCheckpointSequenceNumber = new ExtendedSequenceNumber("999");
        when(streamProxy.getIterator(anyString(), anyString(), anyString())).thenReturn("startingIterator");
        when(leaseManager.getLease(anyString())).thenReturn(null);
        when(leaseCoordinator.getLeaseManager()).thenReturn(leaseManager);
        when(config.getRecordsFetcherFactory()).thenReturn(new SimpleRecordsFetcherFactory());
        when(checkpoint.getCheckpointObject(anyString())).thenReturn(
                new Checkpoint(checkpointSequenceNumber, pendingCheckpointSequenceNumber));

        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        consumer.consumeShard(); // submit BlockOnParentShardTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.WAITING_ON_PARENT_SHARDS)));
        verify(processor, times(0)).initialize(any(InitializationInput.class));

        consumer.consumeShard(); // submit InitializeTask
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.INITIALIZING)));
        verify(processor, times(1)).initialize(argThat(
                initializationInputMatcher(checkpointSequenceNumber, pendingCheckpointSequenceNumber)));
        verify(processor, times(1)).initialize(any(InitializationInput.class)); // no other calls with different args

        consumer.consumeShard();
        Thread.sleep(50L);
        assertThat(consumer.getCurrentState(), is(equalTo(KinesisConsumerStates.ShardConsumerState.PROCESSING)));
    }

    @Test
    public void testCreateSynchronousGetRecordsRetrieval() {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);

        DynamoDBStreamsShardConsumer shardConsumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        Optional.empty(),
                        Optional.empty(),
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        assertEquals(shardConsumer.getGetRecordsCache().getGetRecordsRetrievalStrategy().getClass(),
                SynchronousGetRecordsRetrievalStrategy.class);
    }

    @Test
    public void testCreateAsynchronousGetRecordsRetrieval() {
        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.TRIM_HORIZON);

        DynamoDBStreamsShardConsumer shardConsumer =
                new DynamoDBStreamsShardConsumer(shardInfo,
                        streamConfig,
                        checkpoint,
                        processor,
                        leaseCoordinator,
                        parentShardPollIntervalMillis,
                        cleanupLeasesOfCompletedShards,
                        executorService,
                        metricsFactory,
                        taskBackoffTimeMillis,
                        KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                        Optional.of(1),
                        Optional.of(2),
                        config,
                        shardSyncer,
                        shardSyncStrategy);

        assertEquals(shardConsumer.getGetRecordsCache().getGetRecordsRetrievalStrategy().getClass(),
                AsynchronousGetRecordsRetrievalStrategy.class);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testLongRunningTasks() throws InterruptedException {
        final long sleepTime = 1000L;
        ExecutorService mockExecutorService = mock(ExecutorService.class);
        Future<TaskResult> mockFuture = mock(Future.class);

        when(mockExecutorService.submit(any(ITask.class))).thenReturn(mockFuture);
        when(mockFuture.isDone()).thenReturn(false);
        when(mockFuture.isCancelled()).thenReturn(false);
        when(config.getLogWarningForTaskAfterMillis()).thenReturn(Optional.of(sleepTime));

        ShardInfo shardInfo = new ShardInfo("s-0-0", "testToken", null, ExtendedSequenceNumber.LATEST);

        DynamoDBStreamsShardConsumer shardConsumer = new DynamoDBStreamsShardConsumer(
                shardInfo,
                streamConfig,
                checkpoint,
                processor,
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesOfCompletedShards,
                mockExecutorService,
                metricsFactory,
                taskBackoffTimeMillis,
                KinesisClientLibConfiguration.DEFAULT_SKIP_SHARD_SYNC_AT_STARTUP_IF_LEASES_EXIST,
                config,
                shardSyncer,
                shardSyncStrategy);
        shardConsumer.consumeShard();

        Thread.sleep(sleepTime);

        shardConsumer.consumeShard();

        verify(config).getLogWarningForTaskAfterMillis();
        verify(mockFuture).isDone();
        verify(mockFuture).isCancelled();
    }

    //@formatter:off (gets the formatting wrong)
    private void verifyConsumedRecords(List<Record> expectedRecords,
                                       List<Record> actualRecords) {
        //@formatter:on
        assertThat(actualRecords.size(), is(equalTo(expectedRecords.size())));
        ListIterator<Record> expectedIter = expectedRecords.listIterator();
        ListIterator<Record> actualIter = actualRecords.listIterator();
        for (int i = 0; i < expectedRecords.size(); ++i) {
            assertThat(actualIter.next(), is(equalTo(expectedIter.next())));
        }
    }

    private List<Record> toUserRecords(List<Record> records) {
        if (records == null || records.isEmpty()) {
            return records;
        }
        List<Record> userRecords = new ArrayList<Record>();
        for (Record record : records) {
            userRecords.add(new UserRecord(record));
        }
        return userRecords;
    }

    private KinesisClientLease createLease(String leaseKey, String leaseOwner, Collection<String> parentShardIds) {
        KinesisClientLease lease = new KinesisClientLease();
        lease.setLeaseKey(leaseKey);
        lease.setLeaseOwner(leaseOwner);
        lease.setParentShardIds(parentShardIds);
        return lease;
    }


    Matcher<InitializationInput> initializationInputMatcher(final ExtendedSequenceNumber checkpoint,
                                                            final ExtendedSequenceNumber pendingCheckpoint) {
        return new TypeSafeMatcher<InitializationInput>() {
            @Override
            protected boolean matchesSafely(InitializationInput item) {
                return Objects.equals(checkpoint, item.getExtendedSequenceNumber())
                        && Objects.equals(pendingCheckpoint, item.getPendingCheckpointSequenceNumber());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(String.format("Checkpoint should be %s and pending checkpoint should be %s",
                        checkpoint, pendingCheckpoint));
            }
        };
    }
}
