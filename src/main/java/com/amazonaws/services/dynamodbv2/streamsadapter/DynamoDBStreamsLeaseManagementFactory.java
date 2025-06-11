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

import lombok.NonNull;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.leases.LeaseCleanupManager;
import software.amazon.kinesis.leases.LeaseCoordinator;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public class DynamoDBStreamsLeaseManagementFactory extends DynamoDBLeaseManagementFactory {
    /**
     * Constructor.
     *
     * @param kinesisClient the {@link KinesisAsyncClient}
     * @param leaseManagementConfig the {@link LeaseManagementConfig}
     * @param retrievalConfig the {@link RetrievalConfig}
     */
    public DynamoDBStreamsLeaseManagementFactory(@NonNull KinesisAsyncClient kinesisClient,
                                                 @NonNull LeaseManagementConfig leaseManagementConfig,
                                                 @NonNull RetrievalConfig retrievalConfig) {
        super(
                kinesisClient,
                leaseManagementConfig.dynamoDBClient(),
                leaseManagementConfig.tableName(),
                leaseManagementConfig.workerIdentifier(),
                leaseManagementConfig.executorService(),
                leaseManagementConfig.failoverTimeMillis(),
                leaseManagementConfig.enablePriorityLeaseAssignment(),
                leaseManagementConfig.epsilonMillis,
                leaseManagementConfig.maxLeasesForWorker(),
                leaseManagementConfig.maxLeasesToStealAtOneTime(),
                leaseManagementConfig.maxLeaseRenewalThreads(),
                leaseManagementConfig.cleanupLeasesUponShardCompletion(),
                leaseManagementConfig.ignoreUnexpectedChildShards(),
                leaseManagementConfig.shardSyncIntervalMillis(),
                leaseManagementConfig.consistentReads(),
                leaseManagementConfig.listShardsBackoffTimeInMillis(),
                leaseManagementConfig.maxListShardsRetryAttempts(),
                leaseManagementConfig.maxCacheMissesBeforeReload(),
                leaseManagementConfig.listShardsCacheAllowedAgeInSeconds(),
                leaseManagementConfig.cacheMissWarningModulus(),
                leaseManagementConfig.initialLeaseTableReadCapacity(),
                leaseManagementConfig.initialLeaseTableWriteCapacity(),
                leaseManagementConfig.tableCreatorCallback(),
                leaseManagementConfig.dynamoDbRequestTimeout(),
                leaseManagementConfig.billingMode(),
                leaseManagementConfig.leaseTableDeletionProtectionEnabled(),
                leaseManagementConfig.leaseTablePitrEnabled(),
                leaseManagementConfig.tags(),
                retrievalConfig.streamTracker().isMultiStream() ? new DynamoDBMultiStreamLeaseSerializer()
                        : new DynamoDBLeaseSerializer(),
                leaseManagementConfig.customShardDetectorProvider(),
                retrievalConfig.streamTracker().isMultiStream(),
                leaseManagementConfig.leaseCleanupConfig(),
                leaseManagementConfig.workerUtilizationAwareAssignmentConfig(),
                leaseManagementConfig.gracefulLeaseHandoffConfig(),
                leaseManagementConfig.leaseAssignmentIntervalMillis()
        );
    }

    @Override
    public ShardDetector createShardDetector(StreamConfig streamConfig) {
        return new DynamoDBStreamsShardDetector(
                super.getKinesisClient(),
                streamConfig.streamIdentifier(),
                super.getListShardsCacheAllowedAgeInSeconds(),
                super.getMaxCacheMissesBeforeReload(),
                super.getCacheMissWarningModulus(),
                super.getDynamoDbRequestTimeout()
        );
    }

    @Override
    public ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory,
                                                           StreamConfig streamConfig,
                                                           DeletedStreamListProvider deletedStreamListProvider) {
        return new ShardSyncTaskManager(
                this.createShardDetector(streamConfig),
                this.createLeaseRefresher(),
                streamConfig.initialPositionInStreamExtended(),
                super.isCleanupLeasesUponShardCompletion(),
                super.isIgnoreUnexpectedChildShards(),
                super.getShardSyncIntervalMillis(),
                super.getExecutorService(),
                new DynamoDBStreamsShardSyncer(
                        super.isMultiStreamMode(),
                        streamConfig.streamIdentifier().toString(),
                        super.isCleanupLeasesUponShardCompletion(),
                        deletedStreamListProvider),
                metricsFactory
        );
    }

    // Overriding the lease cleanup manager as we dont have child shards yet and we will be doing lease cleanup in
    // ShardSyncer.
    @Override
    public LeaseCleanupManager createLeaseCleanupManager(MetricsFactory metricsFactory) {
        return new NoOpLeaseCleanupManager(
                createLeaseCoordinator(metricsFactory),
                metricsFactory,
                Executors.newSingleThreadScheduledExecutor(),
                super.isCleanupLeasesUponShardCompletion(),
                super.getLeaseCleanupConfig().leaseCleanupIntervalMillis(),
                super.getLeaseCleanupConfig().completedLeaseCleanupIntervalMillis(),
                super.getLeaseCleanupConfig().garbageLeaseCleanupIntervalMillis());
    }

    /**
     * A no-op implementation of {@link LeaseCleanupManager} that doesn't use any resources
     * Since we are cleaning up the leases from {@link DynamoDBStreamsShardSyncer} we dont want
     * to run this thread to encounter breaking behaviour
     */
    static class NoOpLeaseCleanupManager extends LeaseCleanupManager {
         NoOpLeaseCleanupManager(@NonNull LeaseCoordinator leaseCoordinator,
                                       @NonNull MetricsFactory metricsFactory,
                                       @NonNull ScheduledExecutorService deletionThreadPool,
                                       boolean cleanupLeasesUponShardCompletion,
                                       long leaseCleanupIntervalMillis,
                                       long completedLeaseCleanupIntervalMillis,
                                       long garbageLeaseCleanupIntervalMillis) {
            super(
                    leaseCoordinator,
                    metricsFactory,
                    deletionThreadPool,
                    cleanupLeasesUponShardCompletion,
                    leaseCleanupIntervalMillis,
                    completedLeaseCleanupIntervalMillis,
                    garbageLeaseCleanupIntervalMillis
            );
        }

        @Override
        public void start() {
        }

        @Override
        public void shutdown() {
        }
    }
}
