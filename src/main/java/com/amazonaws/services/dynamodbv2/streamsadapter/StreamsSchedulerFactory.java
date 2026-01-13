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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn;

import com.amazonaws.services.dynamodbv2.streamsadapter.processor.DynamoDBStreamsShardRecordProcessor;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsPollingConfig;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsClientSideCatchUpConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.kinesis.checkpoint.CheckpointConfig;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.CoordinatorConfig;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.lifecycle.LifecycleConfig;
import software.amazon.kinesis.metrics.MetricsConfig;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.ProcessorConfig;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.processor.StreamTracker;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.DataFetcher;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("ParameterNumber")
public final class StreamsSchedulerFactory {

    private StreamsSchedulerFactory() {}
    /**
     * Factory function for customers to create a stream tracker to consume multiple DynamoDB Streams from a single
     * application.
     *
     * For existing KCLv1 applications, while migration, DO NOT use MultiStreamTracker as the lease key format
     * is different for multi-stream use case.
     *
     * @param dynamoDBStreamArns                  the list of DynamoDB stream ARNs
     * @param initialPositionInStreamExtended     the {@link InitialPositionInStreamExtended}
     * @param formerStreamsLeasesDeletionStrategy the {@link FormerStreamsLeasesDeletionStrategy}
     * @return {@link StreamTracker}
     */
    public static StreamTracker createMultiStreamTracker(List<String> dynamoDBStreamArns,
                                                         @NonNull InitialPositionInStreamExtended
                                                                 initialPositionInStreamExtended,
                                                         FormerStreamsLeasesDeletionStrategy
                                                                 formerStreamsLeasesDeletionStrategy) {
        if (CollectionUtils.isEmpty(dynamoDBStreamArns)) {
            throw new IllegalArgumentException("Stream ARN list cannot be empty");
        }

        for (String streamArn : dynamoDBStreamArns) {
            if (!KinesisMapperUtil.isValidDynamoDBStreamArn(streamArn)) {
                throw new IllegalArgumentException("Invalid DynamoDB Stream ARN: " + streamArn);
            }
        }

        if (!(initialPositionInStreamExtended.getInitialPositionInStream() == InitialPositionInStream.LATEST
                || initialPositionInStreamExtended.getInitialPositionInStream() == InitialPositionInStream
                .TRIM_HORIZON)) {
            throw new IllegalArgumentException("Invalid Initial PositionInStream: "
                    + initialPositionInStreamExtended.getInitialPositionInStream());
        }

        List<StreamConfig> streamConfigList = dynamoDBStreamArns.stream()
                .map(streamArn -> new StreamConfig(
                        StreamIdentifier.multiStreamInstance(createKinesisStreamIdentifierFromDynamoDBStreamsArn(
                                streamArn, true)),
                        initialPositionInStreamExtended,
                        null
                ))
                .collect(Collectors.toList());
        return new DynamoDBStreamsMultiStreamTracker(streamConfigList, formerStreamsLeasesDeletionStrategy);
    }

    /**
     * Factory function for customers to create a stream tracker to consume a single DynamoDB Stream from a single
     * application.
     *
     * @param dynamoDBStreamArn the DynamoDB stream ARN
     * @return {@link StreamTracker}
     */
    public static StreamTracker createSingleStreamTracker(
            @NonNull String dynamoDBStreamArn,
            @NonNull InitialPositionInStreamExtended initialPositionInStreamExtended) {
        if (!KinesisMapperUtil.isValidDynamoDBStreamArn(dynamoDBStreamArn)) {
            throw new IllegalArgumentException("Invalid DynamoDB Stream ARN: " + dynamoDBStreamArn);
        }

        if (!(initialPositionInStreamExtended.getInitialPositionInStream() == InitialPositionInStream.LATEST
                || initialPositionInStreamExtended.getInitialPositionInStream() == InitialPositionInStream
                .TRIM_HORIZON)) {
            throw new IllegalArgumentException("Invalid Initial PositionInStream: "
                    + initialPositionInStreamExtended.getInitialPositionInStream());
        }
        return new SingleStreamTracker(createKinesisStreamIdentifierFromDynamoDBStreamsArn(dynamoDBStreamArn,
                false), initialPositionInStreamExtended);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig      the {@link CheckpointConfig}
     * @param coordinatorConfig     the {@link CoordinatorConfig}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param lifecycleConfig       the {@link LifecycleConfig}
     * @param metricsConfig         the {@link MetricsConfig}
     * @param processorConfig       the {@link ProcessorConfig}
     * @param retrievalConfig       the {@link RetrievalConfig}
     * @param credentialsProvider   the {@link AwsCredentialsProvider}
     * @param region                {@link Region}
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AwsCredentialsProvider credentialsProvider,
            @NonNull Region region) {
        AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(
                credentialsProvider, region);

        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, amazonDynamoDBStreamsAdapterClient);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig      the {@link CheckpointConfig}
     * @param coordinatorConfig     the {@link CoordinatorConfig}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param lifecycleConfig       the {@link LifecycleConfig}
     * @param metricsConfig         the {@link MetricsConfig}
     * @param processorConfig       the {@link ProcessorConfig}
     * @param retrievalConfig       the {@link RetrievalConfig}
     * @param credentialsProvider   the {@link AwsCredentialsProvider}
     * @param region                {@link Region}
     * @param catchUpConfig         the {@link DynamoDBStreamsClientSideCatchUpConfig} for automatic polling rate adjustment
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AwsCredentialsProvider credentialsProvider,
            @NonNull Region region,
            @NonNull DynamoDBStreamsClientSideCatchUpConfig catchUpConfig) {
        AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient = new AmazonDynamoDBStreamsAdapterClient(
                credentialsProvider, region);

        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, amazonDynamoDBStreamsAdapterClient, catchUpConfig);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig      the {@link CheckpointConfig}
     * @param coordinatorConfig     the {@link CoordinatorConfig}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param lifecycleConfig       the {@link LifecycleConfig}
     * @param metricsConfig         the {@link MetricsConfig}
     * @param processorConfig       the {@link ProcessorConfig}
     * @param retrievalConfig       the {@link RetrievalConfig}
     * @param dynamoDbStreamsClient the {@link DynamoDbStreamsClient}
     * @param region                the {@link Region}
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull DynamoDbStreamsClient dynamoDbStreamsClient,
            @NonNull Region region) {
        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, dynamoDbStreamsClient, region,
                new DynamoDBStreamsClientSideCatchUpConfig());
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig      the {@link CheckpointConfig}
     * @param coordinatorConfig     the {@link CoordinatorConfig}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param lifecycleConfig       the {@link LifecycleConfig}
     * @param metricsConfig         the {@link MetricsConfig}
     * @param processorConfig       the {@link ProcessorConfig}
     * @param retrievalConfig       the {@link RetrievalConfig}
     * @param dynamoDbStreamsClient the {@link DynamoDbStreamsClient}
     * @param region                the {@link Region}
     * @param catchUpConfig         the {@link DynamoDBStreamsClientSideCatchUpConfig} for automatic polling rate adjustment
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull DynamoDbStreamsClient dynamoDbStreamsClient,
            @NonNull Region region,
            @NonNull DynamoDBStreamsClientSideCatchUpConfig catchUpConfig) {
        AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient =
                new AmazonDynamoDBStreamsAdapterClient(dynamoDbStreamsClient, region);
        return createScheduler(checkpointConfig, coordinatorConfig,
                leaseManagementConfig, lifecycleConfig, metricsConfig,
                processorConfig, retrievalConfig, amazonDynamoDBStreamsAdapterClient, catchUpConfig);
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig      the {@link CheckpointConfig}
     * @param coordinatorConfig     the {@link CoordinatorConfig}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param lifecycleConfig       the {@link LifecycleConfig}
     * @param metricsConfig         the {@link MetricsConfig}
     * @param processorConfig       the {@link ProcessorConfig}
     * @param retrievalConfig       the {@link RetrievalConfig}
     * @param amazonDynamoDBStreamsAdapterClient   the {@link AmazonDynamoDBStreamsAdapterClient}
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient) {

        return createScheduler(checkpointConfig, coordinatorConfig, leaseManagementConfig,
                lifecycleConfig, metricsConfig, processorConfig, retrievalConfig,
                amazonDynamoDBStreamsAdapterClient, new DynamoDBStreamsClientSideCatchUpConfig());
    }

    /**
     * Factory function for customers to create a scheduler.
     * Either createSingleStreamTracker or createMultiStreamTracker must be called before this function.
     *
     * @param checkpointConfig                   the {@link CheckpointConfig}
     * @param coordinatorConfig                  the {@link CoordinatorConfig}
     * @param leaseManagementConfig              the {@link LeaseManagementConfig}
     * @param lifecycleConfig                    the {@link LifecycleConfig}
     * @param metricsConfig                      the {@link MetricsConfig}
     * @param processorConfig                    the {@link ProcessorConfig}
     * @param retrievalConfig                    the {@link RetrievalConfig}
     * @param amazonDynamoDBStreamsAdapterClient the {@link AmazonDynamoDBStreamsAdapterClient}
     * @param catchUpConfig                      the {@link DynamoDBStreamsClientSideCatchUpConfig} for automatic polling rate adjustment
     * @return the {@link Scheduler}
     */
    public static Scheduler createScheduler(
            @NonNull CheckpointConfig checkpointConfig,
            @NonNull CoordinatorConfig coordinatorConfig,
            @NonNull LeaseManagementConfig leaseManagementConfig,
            @NonNull LifecycleConfig lifecycleConfig,
            @NonNull MetricsConfig metricsConfig,
            @NonNull ProcessorConfig processorConfig,
            @NonNull RetrievalConfig retrievalConfig,
            @NonNull AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient,
            @NonNull DynamoDBStreamsClientSideCatchUpConfig catchUpConfig) {
        if (!(processorConfig.shardRecordProcessorFactory().shardRecordProcessor()
                instanceof DynamoDBStreamsShardRecordProcessor)) {
            throw new IllegalArgumentException("ShardRecordProcessor should be of type "
                    + "DynamoDBStreamsShardRecordProcessor");
        }

        if (!(retrievalConfig.retrievalSpecificConfig() instanceof DynamoDBStreamsPollingConfig)) {
            throw new IllegalArgumentException("RetrievalConfig should be of type DynamoDBStreamsPollingConfig");
        }

        Function<DataFetcherProviderConfig, DataFetcher> dataFetcherProvider =
                (dataFetcherProviderConfig) -> new DynamoDBStreamsDataFetcher(
                        amazonDynamoDBStreamsAdapterClient,
                        dataFetcherProviderConfig,
                        catchUpConfig);

        PollingConfig pollingConfig = (PollingConfig) retrievalConfig.retrievalSpecificConfig();
        pollingConfig.dataFetcherProvider(dataFetcherProvider);
        pollingConfig.sleepTimeController(new DynamoDBStreamsSleepTimeController(catchUpConfig, 
                metricsConfig.metricsFactory()));
        retrievalConfig.retrievalSpecificConfig(pollingConfig);

        if (!coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist()) {
            log.warn("skipShardSyncAtWorkerInitializationIfLeasesExist is not set to true. "
                    + "This will cause the worker to delay working on lease. Setting this to true");
            coordinatorConfig.skipShardSyncAtWorkerInitializationIfLeasesExist(true);
        }

        DynamoDBStreamsLeaseManagementFactory dynamoDBStreamsLeaseManagementFactory =
                new DynamoDBStreamsLeaseManagementFactory(
                        amazonDynamoDBStreamsAdapterClient,
                        leaseManagementConfig,
                        retrievalConfig
                );
        leaseManagementConfig.leaseManagementFactory(dynamoDBStreamsLeaseManagementFactory);
        leaseManagementConfig.consumerTaskFactory(new DynamoDBStreamsConsumerTaskFactory());

        if (leaseManagementConfig.leasesRecoveryAuditorInconsistencyConfidenceThreshold() > 0) {
            log.warn("leasesRecoveryAuditorInconsistencyConfidenceThreshold is greater than 0. "
                    + "DynamoDB Streams adapter does not do hole tracking. Setting this to 0.");
            leaseManagementConfig.leasesRecoveryAuditorInconsistencyConfidenceThreshold(0);
        }

        return new Scheduler(
                checkpointConfig,
                coordinatorConfig,
                leaseManagementConfig,
                lifecycleConfig,
                metricsConfig,
                processorConfig,
                retrievalConfig);
    }
}