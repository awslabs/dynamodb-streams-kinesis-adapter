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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName;

import com.amazonaws.services.dynamodbv2.streamsadapter.util.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.ShardGraphTracker;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Synchronized;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceInUseException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Retrieves a Shard object from the cache based on the provided shardId.
 * <pre>
 * Start
 *   │
 *   ▼
 * Is Cache Empty? ──Yes──► Synchronized Block
 *   │                         │
 *   No                       ▼
 *   │                     Initialize Cache
 *   ▼
 * Get Shard from Cache
 *   │
 *   ▼
 * Shard Found? ──No──► Increment Cache Misses
 *   │                         │
 *   Yes                      ▼
 *   │               Need Refresh? ──Yes──► Synchronized Block
 *   │                         │                │
 *   │                        No               ▼
 *   │                         │            Refresh Cache
 *   │                         │                │
 *   │                         │                ▼
 *   │                         │            Reset Counter
 *   │                         │                │
 *   ▼                         ▼                ▼
 * Return Shard ◄────────────────────────────────
 * </pre>
 *
 * shardId The unique identifier of the shard to retrieve
 * @return The Shard object if found, null otherwise
 */

@Slf4j
@Accessors(fluent = true)
public class DynamoDBStreamsShardDetector implements ShardDetector {

    @NonNull
    private final KinesisAsyncClient kinesisAsyncClient;

    @NonNull
    @Getter
    private final StreamIdentifier streamIdentifier;

    private final String streamArn;

    private final long listShardsCacheAllowedAgeInSeconds;
    private final int maxCacheMissesBeforeReload;
    private final int cacheMissWarningModulus;
    private final Duration kinesisRequestTimeout;

    private volatile Map<String, Shard> cachedShardMap = null;
    private volatile Instant lastCacheUpdateTime;

    @Getter(AccessLevel.PACKAGE)
    private final AtomicInteger cacheMisses = new AtomicInteger(0);

    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER;

    static {
        AWS_EXCEPTION_MANAGER = new AWSExceptionManager();
        AWS_EXCEPTION_MANAGER.add(KinesisException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(LimitExceededException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceInUseException.class, t -> t);
        AWS_EXCEPTION_MANAGER.add(ResourceNotFoundException.class, t -> t);
    }

    public DynamoDBStreamsShardDetector(
            @NonNull KinesisAsyncClient kinesisAsyncClient,
            @NonNull StreamIdentifier streamIdentifier,
            long listShardsCacheAllowedAgeInSeconds,
            int maxCacheMissesBeforeReload,
            int cacheMissWarningModulus,
            Duration kinesisRequestTimeout) {
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.streamIdentifier = streamIdentifier;
        this.listShardsCacheAllowedAgeInSeconds = listShardsCacheAllowedAgeInSeconds;
        this.maxCacheMissesBeforeReload = maxCacheMissesBeforeReload;
        this.cacheMissWarningModulus = cacheMissWarningModulus;
        this.kinesisRequestTimeout = kinesisRequestTimeout;
        this.streamArn = createDynamoDBStreamsArnFromKinesisStreamName(this.streamIdentifier.streamName());
    }

    @Override
    public Shard shard(String shardId) {
        if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
            synchronized (this) {
                if (CollectionUtils.isNullOrEmpty(this.cachedShardMap)) {
                    listShards();
                }
            }
        }
        Shard shard = cachedShardMap.get(shardId);

        if (shard == null) {
            if (cacheMisses.incrementAndGet() > maxCacheMissesBeforeReload || shouldRefreshCache()) {
                synchronized (this) {
                    shard = cachedShardMap.get(shardId);

                    if (shard == null) {
                        log.info("Too many shard map cache misses for stream: {} or " +
                                "cache is out of date -- forcing a refresh", streamArn);
                        describeStream(null);
                        shard = cachedShardMap.get(shardId);

                        if (shard == null) {
                            log.warn(
                                    "Even after cache refresh shard '{}' wasn't found. This could indicate a bigger"
                                            + " problem.",
                                    shardId);
                        }
                    }
                    cacheMisses.set(0);
                }
            }
        }

        if (shard == null) {
            final String message =
                    String.format("Cannot find the shard given the shardId %s. Cache misses: %s", shardId, cacheMisses);
            if (cacheMisses.get() % cacheMissWarningModulus == 0) {
                log.warn(message);
            } else {
                log.debug(message);
            }
        }

        return shard;
    }

    @Override
    @Synchronized
    public List<Shard> listShards() {
        DescribeStreamResult describeStreamResult = describeStream(null);
        return describeStreamResult.getShards();
    }

    @Synchronized
    public DescribeStreamResult describeStream(String lastSeenShardId) {
        ShardGraphTracker shardTracker = new ShardGraphTracker();
        String exclusiveStartShardId = lastSeenShardId;
        DescribeStreamResult describeStreamResult = new DescribeStreamResult();
        DescribeStreamResponse describeStreamResponse;

        // Phase 1: Collect all shards from Paginations.
        do {
            describeStreamResponse = describeStreamResponse(exclusiveStartShardId);
            // Collect shards
            shardTracker.collectShards(describeStreamResponse.streamDescription().shards());

            List<Shard> shards = describeStreamResponse.streamDescription().shards();

            describeStreamResult.addStatus(describeStreamResponse.streamDescription().streamStatusAsString());
            if (!shards.isEmpty()) {
                exclusiveStartShardId = shards.get(shards.size() - 1).shardId();
            }
        } while (describeStreamResponse.streamDescription().hasMoreShards());

        if (Objects.equals(describeStreamResult.getStreamStatus(), "ENABLING")) {
            log.warn("Stream: {} is in ENABLING state, new shards will not be discovered until stream gets enabled.",
                    streamArn);
        }

        // Phase 2: Close open parents
        shardTracker.closeOpenParents();

        // Phase 3: Mark leaf shards as active if stream is disabled
        if (Objects.equals(describeStreamResult.getStreamStatus(), "DISABLED")) {
            shardTracker.markLeafShardsActive();
        }

        // Get processed shards and update result
        List<Shard> processedShards = shardTracker.getShards();
        describeStreamResult.addShards(processedShards);

        // Update cache
        cachedShardMap(processedShards);

        return describeStreamResult;
    }

    private DescribeStreamResponse describeStreamResponse(String exclusiveStartShardId) {
        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(this.streamArn)
                .exclusiveStartShardId(exclusiveStartShardId)
                .build();

        DescribeStreamResponse describeStreamResponse;
        try {
            describeStreamResponse = this.kinesisAsyncClient.describeStream(describeStreamRequest).get();
        } catch (ExecutionException e) {
            throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
        } catch (InterruptedException e) {
            log.debug("Interrupted exception caught, shutdown initiated, returning null");
            return null;
        }

        if (describeStreamResponse == null) {
            throw new IllegalStateException("Received null from DescribeStream call.");
        }

        return describeStreamResponse;
    }

    private boolean shouldRefreshCache() {
        final Duration secondsSinceLastUpdate = Duration.between(lastCacheUpdateTime, Instant.now());
        final String message = String.format("Shard map cache for stream: %s is %d seconds old",
                streamArn, secondsSinceLastUpdate.getSeconds());
        if (secondsSinceLastUpdate.compareTo(Duration.of(listShardsCacheAllowedAgeInSeconds, ChronoUnit.SECONDS)) > 0) {
            log.info("{}. Age exceeds limit of {} seconds -- Refreshing.", message, listShardsCacheAllowedAgeInSeconds);
            return true;
        }

        log.debug("{}. Age doesn't exceed limit of {} seconds.", message, listShardsCacheAllowedAgeInSeconds);
        return false;
    }

   private void cachedShardMap(final List<Shard> shards) {
        cachedShardMap = shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
        lastCacheUpdateTime = Instant.now();
    }
}
