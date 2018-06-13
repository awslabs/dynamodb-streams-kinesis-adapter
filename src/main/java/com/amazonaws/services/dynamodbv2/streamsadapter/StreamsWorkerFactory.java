/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.concurrent.ExecutorService;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.NoOpShardPrioritization;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

/**
 * The StreamsWorkerFactory uses the Kinesis Client Library's Worker
 * class to provide convenient constructors for ease-of-use.
 */
public class StreamsWorkerFactory {

    /**
     * Factory method.
     *
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config, ExecutorService execService) {
        AmazonDynamoDBStreamsAdapterClient streamsClient = new AmazonDynamoDBStreamsAdapterClient(
            config.getKinesisCredentialsProvider(),
            config.getKinesisClientConfiguration());
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .execService(execService)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
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
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, AmazonCloudWatch cloudWatchClient, ExecutorService execService) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .execService(execService)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
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
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDB dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .execService(execService)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
            .build();
    }

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param cloudWatchClient       CloudWatch Client for publishing metrics
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
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
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, AmazonCloudWatchClient cloudWatchClient, ExecutorService execService) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .cloudWatchClient(cloudWatchClient)
            .execService(execService)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
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
     */
    public static Worker createDynamoDbStreamsWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient, AmazonDynamoDBClient dynamoDBClient, IMetricsFactory metricsFactory, ExecutorService execService) {
        return new Worker
            .Builder()
            .recordProcessorFactory(recordProcessorFactory)
            .config(config)
            .kinesisClient(streamsClient)
            .dynamoDBClient(dynamoDBClient)
            .metricsFactory(metricsFactory)
            .execService(execService)
            .kinesisProxy(getDynamoDBStreamsProxy(config, streamsClient))
            .shardPrioritization(config.getShardPrioritizationStrategy())
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

}
