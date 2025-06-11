/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DynamoDBStreamsLeaseManagementFactoryTest {

    @Mock
    private AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient;

    @Mock
    private LeaseManagementConfig leaseManagementConfig;

    @Mock
    private RetrievalConfig retrievalConfig;

    @Mock
    private MetricsFactory metricsFactory;

    @Mock
    private StreamTracker streamTracker;

    @Mock
    private TableCreatorCallback tableCreatorCallback;


    private DynamoDBStreamsLeaseManagementFactory factory;
    private static final String STREAM_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);

        // Setup default mocks
        when(leaseManagementConfig.tableName()).thenReturn("leaseTable");
        when(leaseManagementConfig.workerIdentifier()).thenReturn("worker1");
        when(leaseManagementConfig.tableCreatorCallback()).thenReturn(TableCreatorCallback.NOOP_TABLE_CREATOR_CALLBACK);
        when(leaseManagementConfig.billingMode()).thenReturn(BillingMode.PAY_PER_REQUEST);
        when(leaseManagementConfig.dynamoDbRequestTimeout()).thenReturn(Duration.ofSeconds(30));
        when(leaseManagementConfig.initialLeaseTableReadCapacity()).thenReturn(10);
        when(leaseManagementConfig.initialLeaseTableWriteCapacity()).thenReturn(10);
        when(leaseManagementConfig.tags()).thenReturn(Collections.emptyList());
        when(leaseManagementConfig.leaseTableDeletionProtectionEnabled()).thenReturn(false);
        when(leaseManagementConfig.leaseTablePitrEnabled()).thenReturn(false);
        when(leaseManagementConfig.maxLeaseRenewalThreads()).thenReturn(2);
        when(leaseManagementConfig.maxLeasesForWorker()).thenReturn(2);
        when(leaseManagementConfig.maxLeasesToStealAtOneTime()).thenReturn(2);
        when(leaseManagementConfig.gracefulLeaseHandoffConfig()).thenReturn(LeaseManagementConfig.GracefulLeaseHandoffConfig.builder().build());
        // Setup StreamTracker mock
        when(retrievalConfig.streamTracker()).thenReturn(streamTracker);
        when(streamTracker.isMultiStream()).thenReturn(false); // default to single stream mode

        factory = new DynamoDBStreamsLeaseManagementFactory(
                amazonDynamoDBStreamsAdapterClient,
                leaseManagementConfig,
                retrievalConfig
        );
    }

    @Test
    void testCreateLeaseRefresher() {
        // Execute
        LeaseRefresher leaseRefresher = factory.createLeaseRefresher();

        // Verify
        assertNotNull(leaseRefresher);
    }

    @Test
    void testCreateLeaseCoordinator() {
        // Execute
        LeaseCoordinator coordinator = factory.createLeaseCoordinator(new NullMetricsFactory());

        // Verify
        assertNotNull(coordinator);
    }

    @Test
    void testCreateShardDetector() {
        // Setup
        StreamConfig streamConfig = mock(StreamConfig.class);
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(STREAM_ARN, false));
        when(streamConfig.streamIdentifier()).thenReturn(streamIdentifier);

        // Execute
        ShardDetector detector = factory.createShardDetector(streamConfig);

        // Verify
        assertNotNull(detector);
        assertTrue(detector instanceof DynamoDBStreamsShardDetector);
    }

    @Test
    void testCreateShardSyncTaskManager() {
        // Setup
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(
                KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(STREAM_ARN, false));
        StreamConfig streamConfig = mock(StreamConfig.class);
        when(streamConfig.streamIdentifier()).thenReturn(streamIdentifier);
        when(streamConfig.initialPositionInStreamExtended()).thenReturn(
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );

        // Execute
        ShardSyncTaskManager taskManager = factory.createShardSyncTaskManager(
                new NullMetricsFactory(),
                streamConfig,
                null // deletedStreamListProvider
        );

        // Verify
        assertNotNull(taskManager);
    }

    @Test
    void testMultiStreamMode() {
        // Setup multi-stream mode
        when(streamTracker.isMultiStream()).thenReturn(true);

        factory = new DynamoDBStreamsLeaseManagementFactory(
                amazonDynamoDBStreamsAdapterClient,
                leaseManagementConfig,
                retrievalConfig
        );

        StreamConfig streamConfig = mock(StreamConfig.class);
        StreamIdentifier streamIdentifier = StreamIdentifier.multiStreamInstance(
                KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(STREAM_ARN, true)
        );
        when(streamConfig.streamIdentifier()).thenReturn(streamIdentifier);
        when(streamConfig.initialPositionInStreamExtended()).thenReturn(
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );

        // Execute
        ShardSyncTaskManager taskManager = factory.createShardSyncTaskManager(
                new NullMetricsFactory(),
                streamConfig,
                null
        );

        // Verify
        assertNotNull(taskManager);
    }

    @Test
    void testSingleStreamMode() {
        // Setup explicitly set single-stream mode
        when(streamTracker.isMultiStream()).thenReturn(false);

        factory = new DynamoDBStreamsLeaseManagementFactory(
                amazonDynamoDBStreamsAdapterClient,
                leaseManagementConfig,
                retrievalConfig
        );

        StreamConfig streamConfig = mock(StreamConfig.class);
        StreamIdentifier streamIdentifier = StreamIdentifier.singleStreamInstance(
                KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(STREAM_ARN, false));
        when(streamConfig.streamIdentifier()).thenReturn(streamIdentifier);
        when(streamConfig.initialPositionInStreamExtended()).thenReturn(
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );

        // Execute
        ShardSyncTaskManager taskManager = factory.createShardSyncTaskManager(
                new NullMetricsFactory(),
                streamConfig,
                null
        );

        // Verify
        assertNotNull(taskManager);
    }

    @Test
    void testNoOpLeaseCleanupManager() throws Exception {
        // Create a NoOpLeaseCleanupManager instance
        LeaseCoordinator mockLeaseCoordinator = mock(LeaseCoordinator.class);
        MetricsFactory mockMetricsFactory = mock(MetricsFactory.class);
        ScheduledExecutorService mockExecutorService = mock(ScheduledExecutorService.class);

        // Use reflection to get the NoOpLeaseCleanupManager class
        Class<?> noOpLeaseCleanupManagerClass = Class.forName(
                "com.amazonaws.services.dynamodbv2.streamsadapter.DynamoDBStreamsLeaseManagementFactory$NoOpLeaseCleanupManager");

        // Create instance using constructor
        Object noOpManager = noOpLeaseCleanupManagerClass.getDeclaredConstructor(
                LeaseCoordinator.class,
                MetricsFactory.class,
                ScheduledExecutorService.class,
                boolean.class,
                long.class,
                long.class,
                long.class
        ).newInstance(
                mockLeaseCoordinator,
                mockMetricsFactory,
                mockExecutorService,
                true,
                1000L,
                2000L,
                3000L
        );

        // Call start() and shutdown()
        noOpLeaseCleanupManagerClass.getMethod("start").invoke(noOpManager);
        noOpLeaseCleanupManagerClass.getMethod("shutdown").invoke(noOpManager);

        // Verify that the executor service was not used
        verify(mockExecutorService, never()).scheduleWithFixedDelay(
                any(Runnable.class),
                anyLong(),
                anyLong(),
                any(TimeUnit.class));
    }
}