package com.amazonaws.services.dynamodbv2.streamsadapter.ddblocal;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;

import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsSchedulerFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DynamoDBStreamsProcessRecordsInput;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsPollingConfig;
import com.amazonaws.services.dynamodbv2.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;

/**
 * Scheduler lifecycle integration test for the DynamoDB Streams Kinesis Adapter.
 * Validates the full KCL pipeline: shard detection → lease management →
 * record fetching → record processing → graceful shutdown.
 */
public class DynamoDBStreamsKinesisAdapterIntegrationTest extends DynamoDBStreamsLocalTestBase {

    @Test
    public void testSchedulerLifecycle() throws Exception {
        String tableName = "IntegrationTestTable";
        String streamArn = createTableWithStream(tableName);
        String appName = "IntegrationTest";

        AtomicBoolean initialized = new AtomicBoolean(false);
        AtomicInteger recordCount = new AtomicInteger(0);

        ShardRecordProcessorFactory processorFactory = () -> new DynamoDBStreamsShardRecordProcessor() {
            @Override
            public void initialize(InitializationInput initializationInput) {
                initialized.set(true);
            }

            @Override
            public void processRecords(DynamoDBStreamsProcessRecordsInput processRecordsInput) {
                recordCount.addAndGet(processRecordsInput.records().size());
            }

            @Override
            public void leaseLost(LeaseLostInput leaseLostInput) {
            }

            @Override
            public void shardEnded(ShardEndedInput shardEndedInput) {
                try {
                    shardEndedInput.checkpointer().checkpoint();
                } catch (Exception e) {
                    // ignore in test
                }
            }

            @Override
            public void shutdownRequested(software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput input) {
                try {
                    input.checkpointer().checkpoint();
                } catch (Exception e) {
                    // ignore in test
                }
            }
        };

        DynamoDbAsyncClient dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                .endpointOverride(LOCAL_ENDPOINT)
                .region(REGION)
                .credentialsProvider(CREDENTIALS)
                .build();

        AmazonDynamoDBStreamsAdapterClient schedulerAdapterClient =
                new AmazonDynamoDBStreamsAdapterClient(streamsClient, REGION);

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                StreamsSchedulerFactory.createSingleStreamTracker(streamArn,
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)),
                appName,
                schedulerAdapterClient,
                dynamoDbAsyncClient,
                CloudWatchAsyncClient.builder()
                        .endpointOverride(LOCAL_ENDPOINT)
                        .region(REGION)
                        .credentialsProvider(CREDENTIALS)
                        .build(),
                "testWorker",
                processorFactory);

        DynamoDBStreamsPollingConfig pollingConfig =
                new DynamoDBStreamsPollingConfig(streamArn, schedulerAdapterClient);
        RetrievalConfig retrievalConfig = configsBuilder.retrievalConfig();
        retrievalConfig.retrievalSpecificConfig(pollingConfig);

        Scheduler scheduler = StreamsSchedulerFactory.createScheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsFactory(new NullMetricsFactory()),
                configsBuilder.processorConfig(),
                retrievalConfig,
                schedulerAdapterClient);

        putItem(tableName, "s1", "value1");
        putItem(tableName, "s2", "value2");

        new Thread(scheduler, "kcl-scheduler").start();

        try {
            await().atMost(Duration.ofSeconds(180))
                    .untilAsserted(() -> assertEquals(2, recordCount.get()));
        } finally {
            scheduler.startGracefulShutdown().get(30, TimeUnit.SECONDS);
            dynamoDbAsyncClient.close();
        }
    }
}
