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

import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsCatchUpConfig;
import com.amazonaws.services.dynamodbv2.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsPollingConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.PollingConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

class StreamsSchedulerFactoryTest {

    private RetrievalConfig retrievalConfig;

    private DynamoDBStreamsPollingConfig pollingConfig;

    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Mock
    private ShardRecordProcessorFactory shardRecordProcessorFactory;

    @Mock
    private DynamoDBStreamsShardRecordProcessor shardRecordProcessor;

    private AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient;

    @Mock
    private CloudWatchAsyncClient cloudWatchAsyncClient;

    @Mock
    private DynamoDbAsyncClient dynamoDbAsyncClient;

    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    private Region region = Region.US_WEST_2;

    private static final String VALID_STREAM_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
    private static final String INVALID_STREAM_ARN = "invalid:arn:format";
    private static final String STREAM_NAME = "streamName";
    private static final String APP_NAME = "testApp";
    private static final String WORKER_ID = "testWorker";
    private static final long IDLE_TIME_BETWEEN_READS = 500;


    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        amazonDynamoDBStreamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(awsCredentialsProvider, region);
        pollingConfig = new DynamoDBStreamsPollingConfig(amazonDynamoDBStreamsAdapterClient);
        retrievalConfig = new RetrievalConfig(amazonDynamoDBStreamsAdapterClient, STREAM_NAME, APP_NAME);
        retrievalConfig.retrievalSpecificConfig(pollingConfig);
        when(shardRecordProcessorFactory.shardRecordProcessor()).thenReturn(shardRecordProcessor);
    }

    @Test
    void testCreateMultiStreamTracker_TRIM_HORIZON() {
        List<String> streamArns = Arrays.asList(
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000",
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table2/stream/2024-02-03T00:00:00.000"
        );

        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        FormerStreamsLeasesDeletionStrategy deletionStrategy =
                new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

        StreamTracker tracker = StreamsSchedulerFactory.createMultiStreamTracker(
                streamArns,
                position,
                deletionStrategy
        );

        assertNotNull(tracker);
        assertTrue(tracker instanceof DynamoDBStreamsMultiStreamTracker);

        DynamoDBStreamsMultiStreamTracker multiStreamTracker = (DynamoDBStreamsMultiStreamTracker) tracker;
        List<StreamConfig> configs = multiStreamTracker.streamConfigList();

        assertEquals(2, configs.size());
        assertEquals(deletionStrategy, multiStreamTracker.formerStreamsLeasesDeletionStrategy());
    }

    @Test
    void testCreateMultiStreamTracker_LATEST() {
        List<String> streamArns = Arrays.asList(
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000",
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table2/stream/2024-02-03T00:00:00.000"
        );

        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);
        FormerStreamsLeasesDeletionStrategy deletionStrategy =
                new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

        StreamTracker tracker = StreamsSchedulerFactory.createMultiStreamTracker(
                streamArns,
                position,
                deletionStrategy
        );

        assertNotNull(tracker);
        assertTrue(tracker instanceof DynamoDBStreamsMultiStreamTracker);

        DynamoDBStreamsMultiStreamTracker multiStreamTracker = (DynamoDBStreamsMultiStreamTracker) tracker;
        List<StreamConfig> configs = multiStreamTracker.streamConfigList();

        assertEquals(2, configs.size());
        assertEquals(deletionStrategy, multiStreamTracker.formerStreamsLeasesDeletionStrategy());
    }

    @Test
    void testCreateMultiStreamTracker_InvalidInitialPositionInStream() {
        List<String> streamArns = Arrays.asList(
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000",
                "arn:aws:dynamodb:us-west-2:123456789012:table/Table2/stream/2024-02-03T00:00:00.000"
        );

        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date());
        FormerStreamsLeasesDeletionStrategy deletionStrategy =
                new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createMultiStreamTracker(
                        streamArns, position, deletionStrategy)
        );
    }

    @Test
    void testCreateMultiStreamTrackerWithInvalidArn() {
        List<String> streamArns = Arrays.asList(VALID_STREAM_ARN, INVALID_STREAM_ARN);

        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
        FormerStreamsLeasesDeletionStrategy deletionStrategy =
                new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createMultiStreamTracker(streamArns, position, deletionStrategy)
        );

        assertEquals("Invalid DynamoDB Stream ARN: " + INVALID_STREAM_ARN, exception.getMessage());
    }

    @Test
    void testCreateSingleStreamTracker_TRIM_HORIZON() {
        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

        StreamTracker tracker = StreamsSchedulerFactory.createSingleStreamTracker(VALID_STREAM_ARN, position);

        assertNotNull(tracker);
        assertTrue(tracker.isMultiStream() == false);
    }

    @Test
    void testCreateSingleStreamTracker_LATEST() {
        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST);

        StreamTracker tracker = StreamsSchedulerFactory.createSingleStreamTracker(VALID_STREAM_ARN, position);

        assertNotNull(tracker);
        assertTrue(tracker.isMultiStream() == false);
    }

    @Test
    void testCreateSingleStreamTracker_InvalidInitialPositionInStream() {
        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPositionAtTimestamp(new Date());

        assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createSingleStreamTracker(VALID_STREAM_ARN, position)
        );
    }

    @Test
    void testCreateSingleStreamTrackerWithInvalidArn() {
        InitialPositionInStreamExtended position =
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createSingleStreamTracker(INVALID_STREAM_ARN, position)
        );

        assertEquals("Invalid DynamoDB Stream ARN: " + INVALID_STREAM_ARN, exception.getMessage());
    }

    @Test
    void testCreateMultiStreamTrackerWithEmptyStreamList() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createMultiStreamTracker(
                        Collections.emptyList(),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                        new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy()
                )
        );
        assertEquals("Stream ARN list cannot be empty", exception.getMessage());
    }

    @Test
    void testCreateMultiStreamTrackerWithNullStreamList() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createMultiStreamTracker(
                        null,
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON),
                        new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy()
                )
        );
        assertEquals("Stream ARN list cannot be empty", exception.getMessage());
    }

    @Test
    void testCreateMultiStreamTrackerWithNullInitialPosition() {
        List<String> streamArns = Arrays.asList(VALID_STREAM_ARN);
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> StreamsSchedulerFactory.createMultiStreamTracker(
                        streamArns,
                        null,
                        new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy()
                )
        );
        assertEquals("initialPositionInStreamExtended is marked non-null but is null", exception.getMessage());
    }

    @Test
    void testCreateScheduler() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        pollingConfig.idleTimeBetweenReadsInMillis(IDLE_TIME_BETWEEN_READS);
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                credentialsProvider,
                region
        );

        assertNotNull(scheduler);
        assertTrue(scheduler.coordinatorConfig().skipShardSyncAtWorkerInitializationIfLeasesExist());
        assertEquals(IDLE_TIME_BETWEEN_READS, ((PollingConfig)scheduler.retrievalConfig().retrievalSpecificConfig()).idleTimeBetweenReadsInMillis());
    }

    @Test
    void testCreateSchedulerDefaultPollingConfig() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                credentialsProvider,
                region
        );

        PollingConfig actualPollingConfig = (PollingConfig) scheduler.retrievalConfig().retrievalSpecificConfig();
        assertEquals(1000, actualPollingConfig.idleTimeBetweenReadsInMillis());
    }

    @Test
    void testCreateScheduleOverridenPollingConfig() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        pollingConfig.idleTimeBetweenReadsInMillis(IDLE_TIME_BETWEEN_READS);
        retrievalConfig.retrievalSpecificConfig(pollingConfig);
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                credentialsProvider,
                region
        );

        PollingConfig actualPollingConfig = (PollingConfig) scheduler.retrievalConfig().retrievalSpecificConfig();
        assertEquals(IDLE_TIME_BETWEEN_READS, actualPollingConfig.idleTimeBetweenReadsInMillis());
    }

    @Test
    void testCreateScheduleOverridenPollingConfigLessThan200Millis() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        pollingConfig.idleTimeBetweenReadsInMillis(150);
        retrievalConfig.retrievalSpecificConfig(pollingConfig);
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                credentialsProvider,
                region
        );

        PollingConfig actualPollingConfig = (PollingConfig) scheduler.retrievalConfig().retrievalSpecificConfig();
        assertEquals(150, actualPollingConfig.idleTimeBetweenReadsInMillis());
    }

    @Test
    void testCreateScheduler_InvalidParams() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);

        assertThrows(IllegalArgumentException.class,
                () -> StreamsSchedulerFactory.createScheduler(
                        configsBuilder.checkpointConfig(),
                        configsBuilder.coordinatorConfig(),
                        configsBuilder.leaseManagementConfig(),
                        configsBuilder.lifecycleConfig(),
                        configsBuilder.metricsConfig(),
                        configsBuilder.processorConfig(),
                        configsBuilder.retrievalConfig(),
                        credentialsProvider,
                        region
        ));
    }

    @Test
    void testCreateSingleStreamTrackerWithNullStreamArn() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> StreamsSchedulerFactory.createSingleStreamTracker(
                        null,
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
                )
        );
        assertEquals("dynamoDBStreamArn is marked non-null but is null", exception.getMessage());
    }

    @Test
    void testCreateSingleStreamTrackerWithNullInitialPosition() {
        NullPointerException exception = assertThrows(
                NullPointerException.class,
                () -> StreamsSchedulerFactory.createSingleStreamTracker(
                        VALID_STREAM_ARN,
                        null
                )
        );
        assertEquals("initialPositionInStreamExtended is marked non-null but is null", exception.getMessage());
    }

    @Test
    void testCreateSchedulerWithCatchUpConfig() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        
        DynamoDBStreamsCatchUpConfig catchUpConfig = new DynamoDBStreamsCatchUpConfig();
        
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                credentialsProvider,
                region,
                catchUpConfig
        );

        assertNotNull(scheduler);
        assertTrue(scheduler.coordinatorConfig().skipShardSyncAtWorkerInitializationIfLeasesExist());
    }

    @Test
    void testCreateSchedulerWithAdapterClientAndCatchUpConfig() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        
        DynamoDBStreamsCatchUpConfig catchUpConfig = new DynamoDBStreamsCatchUpConfig();
        
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                amazonDynamoDBStreamsAdapterClient,
                catchUpConfig
        );

        assertNotNull(scheduler);
        assertTrue(scheduler.coordinatorConfig().skipShardSyncAtWorkerInitializationIfLeasesExist());
    }

    @Test
    void testCreateSchedulerWithDynamoDbStreamsClientAndCatchUpConfig() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        
        DynamoDBStreamsCatchUpConfig catchUpConfig = new DynamoDBStreamsCatchUpConfig();
        
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                dynamoDbStreamsClient,
                region,
                catchUpConfig
        );

        assertNotNull(scheduler);
        assertTrue(scheduler.coordinatorConfig().skipShardSyncAtWorkerInitializationIfLeasesExist());
    }

    @Test
    void testCreateSchedulerWithDynamoDbStreamsClient() {
        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                VALID_STREAM_ARN,
                APP_NAME,
                amazonDynamoDBStreamsAdapterClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                WORKER_ID,
                shardRecordProcessorFactory);
        
        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                retrievalConfig,
                dynamoDbStreamsClient,
                region
        );

        assertNotNull(scheduler);
        assertTrue(scheduler.coordinatorConfig().skipShardSyncAtWorkerInitializationIfLeasesExist());
    }
}