/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * Helper class to create Shard, SequenceRange and related objects.
 * Copied as-is from Kinesis Client Library.
 */
public class ShardObjectHelper {

    private static final int EXPONENT = 128;

    /**
     * Max value of a sequence number (2^128 -1). Useful for defining sequence number range for a shard.
     */
    static final String MAX_SEQUENCE_NUMBER = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE).toString();

    /**
     * Min value of a sequence number (0). Useful for defining sequence number range for a shard.
     */
    static final String MIN_SEQUENCE_NUMBER = BigInteger.ZERO.toString();

    /**
     * Max value of a hash key (2^128 -1). Useful for defining hash key range for a shard.
     */
    public static final String MAX_HASH_KEY = new BigInteger("2").pow(EXPONENT).subtract(BigInteger.ONE).toString();

    /**
     * Min value of a hash key (0). Useful for defining sequence number range for a shard.
     */
    public static final String MIN_HASH_KEY = BigInteger.ZERO.toString();

    /**
     *
     */
    private ShardObjectHelper() {
    }


    /** Helper method to create a new shard object.
     * @param shardId
     * @param parentShardId
     * @param adjacentParentShardId
     * @param sequenceNumberRange
     * @return
     */
    public static Shard newShard(String shardId,
        String parentShardId,
        String adjacentParentShardId,
        SequenceNumberRange sequenceNumberRange) {
        return newShard(shardId, parentShardId, adjacentParentShardId, sequenceNumberRange, null);
    }

    /** Helper method to create a new shard object.
     * @param shardId
     * @param parentShardId
     * @param adjacentParentShardId
     * @param sequenceNumberRange
     * @param hashKeyRange
     * @return
     */
    public static Shard newShard(String shardId,
        String parentShardId,
        String adjacentParentShardId,
        SequenceNumberRange sequenceNumberRange,
        HashKeyRange hashKeyRange) {
        Shard shard = new Shard();
        shard.setShardId(shardId);
        shard.setParentShardId(parentShardId);
        shard.setAdjacentParentShardId(adjacentParentShardId);
        shard.setSequenceNumberRange(sequenceNumberRange);
        shard.setHashKeyRange(hashKeyRange);

        return shard;
    }

    /** Helper method.
     * @param startingSequenceNumber
     * @param endingSequenceNumber
     * @return
     */
    public static SequenceNumberRange newSequenceNumberRange(String startingSequenceNumber, String endingSequenceNumber) {
        SequenceNumberRange range = new SequenceNumberRange();
        range.setStartingSequenceNumber(startingSequenceNumber);
        range.setEndingSequenceNumber(endingSequenceNumber);
        return range;
    }

    /** Helper method.
     * @param startingHashKey
     * @param endingHashKey
     * @return
     */
    public static HashKeyRange newHashKeyRange(String startingHashKey, String endingHashKey) {
        HashKeyRange range = new HashKeyRange();
        range.setStartingHashKey(startingHashKey);
        range.setEndingHashKey(endingHashKey);
        return range;
    }

    public static List<String> getParentShardIds(Shard shard) {
        List<String> parentShardIds = new ArrayList<>(2);
        if (shard.getAdjacentParentShardId() != null) {
            parentShardIds.add(shard.getAdjacentParentShardId());
        }
        if (shard.getParentShardId() != null) {
            parentShardIds.add(shard.getParentShardId());
        }
        return parentShardIds;
    }
}

