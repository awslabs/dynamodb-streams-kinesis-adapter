/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;

/**
 * A uniquely identified group of data records in a DynamoDB
 * stream.
 */
public class ShardAdapter extends Shard {

    private com.amazonaws.services.dynamodbv2.model.Shard internalShard;

    /**
     * Constructs a new shard description using a DynamoDBStreams object.
     *
     * @param shard Instance of DynamoDBStreams Shard
     */
    public ShardAdapter(com.amazonaws.services.dynamodbv2.model.Shard shard) {
        internalShard = shard;
    }

    /**
     * @return The unique identifier of the shard within the DynamoDB stream.
     */
    @Override
    public String getShardId() {
        return internalShard.getShardId();
    }

    @Override
    public void setShardId(String shardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Shard withShardId(String shardId) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The shard Id of the shard's parent.
     */
    @Override
    public String getParentShardId() {
        return internalShard.getParentShardId();
    }

    @Override
    public void setParentShardId(String parentShardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Shard withParentShardId(String parentShardId) {
        throw new UnsupportedOperationException();
    }

    /**
     * The Kinesis model provides an adjacent parent shard ID in the event of
     * a parent shard merge. Since DynamoDB Streams does not support merge, this
     * always returns null.
     *
     * @return The shard Id of the shard adjacent to the shard's parent.
     */
    @Override
    public String getAdjacentParentShardId() {
        return null;
    }

    @Override
    public void setAdjacentParentShardId(String adjacentParentShardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Shard withAdjacentParentShardId(String adjacentParentShardId) {
        throw new UnsupportedOperationException();
    }

    /**
     * The underlying DynamoDB Streams model does not expose hash key range. To
     * ensure compatibility with the Kinesis Client Library, this method
     * returns dummy values.
     *
     * @return The range of possible hash key values for the shard.
     */
    @Override
    public HashKeyRange getHashKeyRange() {
        HashKeyRange hashKeyRange = new HashKeyRange();
        hashKeyRange.setStartingHashKey(java.math.BigInteger.ZERO.toString());
        hashKeyRange.setEndingHashKey(java.math.BigInteger.ONE.toString());
        return hashKeyRange;
    }

    @Override
    public void setHashKeyRange(HashKeyRange hashKeyRange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Shard withHashKeyRange(HashKeyRange hashKeyRange) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The range of possible sequence numbers for the shard.
     */
    @Override
    public SequenceNumberRange getSequenceNumberRange() {
        SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
        sequenceNumberRange.setStartingSequenceNumber(internalShard.getSequenceNumberRange().getStartingSequenceNumber());
        sequenceNumberRange.setEndingSequenceNumber(internalShard.getSequenceNumberRange().getEndingSequenceNumber());
        return sequenceNumberRange;
    }

    @Override
    public void setSequenceNumberRange(SequenceNumberRange sequenceNumberRange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Shard withSequenceNumberRange(SequenceNumberRange sequenceNumberRange) {
        throw new UnsupportedOperationException();
    }

}
