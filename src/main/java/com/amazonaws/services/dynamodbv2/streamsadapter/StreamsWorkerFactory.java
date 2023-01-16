/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.concurrent.ExecutorService;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.leases.StreamsLeaseTaker;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.LeaderDecider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategyType;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncTask;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.util.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker.getMetricsFactory;

/**
 * The StreamsWorkerFactory uses the Kinesis Client Library's Worker
 * class to provide convenient constructors for ease-of-use.
 */
public class StreamsWorkerFactory {
    private static final Log LOG = LogFactory.getLog(StreamsWorkerFactory.class);
    /**
     * Factory method.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config, ExecutorService execService) {
        AmazonDynamoDBStreamsAdapterClient streamsClient = new AmazonDynamoDBStreamsAdapterClient(
            config.getKinesisCredentialsProvider(),
            config.getKinesisClientConfiguration());
        AmazonDynamoDB dynamoDBClient = createClient(AmazonDynamoDBClientBuilder.standard(),
            config.getDynamoDBCredentialsProvider(),
            config.getDynamoDBClientConfiguration(),
            config.getDynamoDBEndpoint(),
            config.getRegionName());
        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);

        AmazonCloudWatch cloudWatchClient = createClient(AmazonCloudWatchClientBuilder.standard(),
                config.getCloudWatchCredentialsProvider(),
                config.getCloudWatchClientConfiguration(),
                null,
                config.getRegionName());
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .execService(execService)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());
        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory         Metrics factory used to emit metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        IMetricsFactory metricsFactory = getMetricsFactory(cloudWatchClient, config);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory         Metrics factory used to emit metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
                                                     AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {

        KinesisClientLeaseManager kinesisClientLeaseManager = new KinesisClientLeaseManager(config.getTableName(), dynamoDBClient, config.getBillingMode());

        boolean isAuditorMode = config.getShardSyncStrategyType() != ShardSyncStrategyType.PERIODIC;

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
        ShardSyncer shardSyncer= new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());
        LeaderDecider leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, kinesisClientLeaseManager);

        DynamoDBStreamsPeriodicShardSyncManager dynamoDBStreamsPeriodicShardSyncManager = new DynamoDBStreamsPeriodicShardSyncManager(config.getWorkerIdentifier(),
                leaderDecider,
                new ShardSyncTask(dynamoDBStreamsProxy,
                        kinesisClientLeaseManager,
                        config.getInitialPositionInStreamExtended(),
                        config.shouldCleanupLeasesUponShardCompletion(),
                        config.shouldIgnoreUnexpectedChildShards(),
                        0 /* shardSyncTaskIdleTimeMillis*/,
                        shardSyncer,
                        null /*latestShards*/),
                metricsFactory,
                kinesisClientLeaseManager,
                dynamoDBStreamsProxy,
                isAuditorMode,
                config.getLeasesRecoveryAuditorExecutionFrequencyMillis(),
                config.getLeasesRecoveryAuditorInconsistencyConfidenceThreshold());

        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .periodicShardSyncManager(dynamoDBStreamsPeriodicShardSyncManager)
            .shardConsumerFactory(new DynamoDBStreamsShardConsumerFactory())
            .execService(execService)
            .kinesisProxy(dynamoDBStreamsProxy)
            .shardSyncer(shardSyncer)
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .leaseManager(kinesisClientLeaseManager)
            .leaseTaker(new StreamsLeaseTaker<>(kinesisClientLeaseManager, config.getWorkerIdentifier(), config.getFailoverTimeMillis())
                    .maxLeasesForWorker(config.getMaxLeasesForWorker()))
            .leaderDecider(leaderDecider)
            .build();
    }

    private static DynamoDBStreamsProxy getDynamoDBStreamsProxy(KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient) {
        return new DynamoDBStreamsProxy.Builder(
            config.getStreamName(),
            config.getKinesisCredentialsProvider(),
            streamsClient)
            .build();
    }

    /*
     * Method to create AWS using provided builders.
     * @param builder Builder used to construct the client object.
     * @param credentialsProvider Provides credentials to access AWS services
     * @param clientConfiguration client Configuration that will be used by the client object.
     * @param endpointUrl The endpoint used for communication
     * @param region The region name for the service.
     */
    static private <R, T extends AwsClientBuilder<T, R>> R createClient(final T builder,
        final AWSCredentialsProvider credentialsProvider,
        final ClientConfiguration clientConfiguration,
        final String endpointUrl,
        final String region) {
        if (credentialsProvider != null) {
            builder.withCredentials(credentialsProvider);
        }
        if (clientConfiguration != null) {
            builder.withClientConfiguration(clientConfiguration);
        }
        if (!StringUtils.isNullOrEmpty(endpointUrl)) {
            LOG.warn("Received configuration for endpoint as " + endpointUrl + ", and region as "
                + region + ".");
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointUrl, region));
        } else if (!StringUtils.isNullOrEmpty(region)) {
            LOG.warn("Received configuration for region as " + region + ".");
            builder.withRegion(region);
        } else {
            LOG.warn("No configuration received for endpoint and region, will default region to us-east-1");
            builder.withRegion(Regions.US_EAST_1);
        }
        return builder.build();
    }
}
