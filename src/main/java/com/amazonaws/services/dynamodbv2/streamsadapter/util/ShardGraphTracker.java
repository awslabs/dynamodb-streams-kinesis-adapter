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
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for managing DynamoDB stream shard relationships and states.
 * Handles parent-child relationships and shard state transitions(i.e closing Open Parent
 * , Opening Leaf Nodes for Disabled Streams)
 * for stream processing.
 *  Note: This class is NOT thread-safe by itself. Thread safety should be
 *  handled by the calling context (e.g., DynamoDBStreamsShardDetector's
 *  synchronized methods).
 */
public class ShardGraphTracker {
    private final Map<String, Shard> shardByShardId = new HashMap<>();

    // Used to mark artificially closed parent shards.
    private static final String END_SEQUENCE_NUMBER_TO_CLOSE_OPEN_PARENT = String.valueOf(Long.MAX_VALUE);

    /**
     * Collects and stores shards for processing. This is the first phase of shard processing
     * where all shards are gathered before any state modifications.
     *
     * @param shards List of shards to be processed. Can be null or empty.
     */
    public void collectShards(List<Shard> shards) {
        if (shards != null && !shards.isEmpty()) {
            shards.forEach(shard -> shardByShardId.put(shard.shardId(), shard));
        }
    }

    /**
     * Processes parent-child relationships by closing open parent shards that have children.
     * A parent shard is considered open if it has no ending sequence number.
     * When closed, the parent shard's ending sequence number is set to maximum value.
     * This is the second phase of shard processing.
     */
    public void closeOpenParents() {
        Set<String> parentShardIds = shardByShardId.values().stream()
                .map(Shard::parentShardId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        shardByShardId.forEach((shardId, shard) -> {
            if (isOpenParentWithChildren(shard, parentShardIds)) {
                shardByShardId.put(shardId, createUpdatedShard(shard, END_SEQUENCE_NUMBER_TO_CLOSE_OPEN_PARENT));
            }
        });
    }

    /**
     * Marks leaf shards as active by removing their ending sequence numbers.
     * A shard is considered a leaf if it is not a parent to any other shard.
     * This is typically used for disabled streams where leaf shards need to remain active.
     * This is the third phase of shard processing.
     */
    public void markLeafShardsActive() {
        Set<String> parentShardIds = shardByShardId.values().stream()
                .map(Shard::parentShardId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        shardByShardId.forEach((shardId, shard) -> {
            if (!parentShardIds.contains(shardId)) {
                shardByShardId.put(shardId, createUpdatedShard(shard, null));
            }
        });
    }

    /**
     * Creates a new shard with updated sequence number range while preserving other attributes.
     *
     * @param original The original shard to be updated
     * @param endSequenceNumber The new ending sequence number (null for open-ended)
     * @return A new shard instance with updated sequence range
     */
    private Shard createUpdatedShard(Shard original, String endSequenceNumber) {
        SequenceNumberRange newRange = SequenceNumberRange.builder()
                .startingSequenceNumber(original.sequenceNumberRange().startingSequenceNumber())
                .endingSequenceNumber(endSequenceNumber)
                .build();

        return Shard.builder()
                .shardId(original.shardId())
                .parentShardId(original.parentShardId())
                .sequenceNumberRange(newRange)
                .hashKeyRange(original.hashKeyRange())
                .adjacentParentShardId(original.adjacentParentShardId())
                .build();
    }

    /**
     * Determines if a shard is a parent with child shards and is currently open.
     *
     * @param shard The shard to check
     * @param parentShardIds Set of all parent shard IDs in the system
     * @return true if the shard is an open parent, false otherwise
     */
    private boolean isOpenParentWithChildren(Shard shard, Set<String> parentShardIds) {
        return parentShardIds.contains(shard.shardId()) && isShardOpen(shard);
    }

    /**
     * Checks if a shard is open (has no ending sequence number).
     *
     * @param shard The shard to check
     * @return true if the shard is open, false if it's closed
     */
    private boolean isShardOpen(Shard shard) {
        return shard.sequenceNumberRange() != null
                && shard.sequenceNumberRange().endingSequenceNumber() == null;
    }

    /**
     * Retrieves the list of all processed shards with their final states.
     * This should be called after all necessary processing phases are complete.
     *
     * @return A new list containing all processed shards
     */
    public List<Shard> getShards() {
        return new ArrayList<>(shardByShardId.values());
    }
}
