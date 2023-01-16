package com.amazonaws.services.dynamodbv2.streamsadapter;


import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.IShardConsumer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.IShardConsumerFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibLeaseCoordinator;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.RecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardInfo;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncStrategy;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShardSyncer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.StreamConfig;
import com.amazonaws.services.kinesis.leases.impl.LeaseCleanupManager;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class DynamoDBStreamsShardConsumerFactory implements IShardConsumerFactory {


    public DynamoDBStreamsShardConsumerFactory(){};

    @Override
    public IShardConsumer createShardConsumer(ShardInfo shardInfo,
                                              StreamConfig streamConfig,
                                              ICheckpoint checkpointTracker,
                                              IRecordProcessor recordProcessor,
                                              RecordProcessorCheckpointer recordProcessorCheckpointer,
                                              KinesisClientLibLeaseCoordinator leaseCoordinator,
                                              long parentShardPollIntervalMillis,
                                              boolean cleanupLeasesUponShardCompletion,
                                              ExecutorService executorService,
                                              IMetricsFactory metricsFactory,
                                              long taskBackoffTimeMillis,
                                              boolean skipShardSyncAtWorkerInitializationIfLeasesExist,
                                              Optional<Integer> retryGetRecordsInSeconds,
                                              Optional<Integer> maxGetRecordsThreadPool,
                                              KinesisClientLibConfiguration config, ShardSyncer shardSyncer, ShardSyncStrategy shardSyncStrategy,
                                              LeaseCleanupManager leaseCleanupManager) {
        return new DynamoDBStreamsShardConsumer(shardInfo,
                streamConfig,
                checkpointTracker,
                recordProcessor,
                recordProcessorCheckpointer,
                leaseCoordinator,
                parentShardPollIntervalMillis,
                cleanupLeasesUponShardCompletion,
                executorService,
                metricsFactory,
                taskBackoffTimeMillis,
                skipShardSyncAtWorkerInitializationIfLeasesExist,
                new DynamoDBStreamsDataFetcher(streamConfig.getStreamProxy(), shardInfo),
                retryGetRecordsInSeconds,
                maxGetRecordsThreadPool,
                config, shardSyncer, shardSyncStrategy,
                leaseCleanupManager);
    }
}
