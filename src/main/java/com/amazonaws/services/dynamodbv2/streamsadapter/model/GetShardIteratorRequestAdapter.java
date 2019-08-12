/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;

/**
 * Container for the parameters to the GetShardIterator operation.
 */
public class GetShardIteratorRequestAdapter extends GetShardIteratorRequest {
    // Evaluate each ShardIteratorType toString() only once.
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_AT_SEQUENCE_NUMBER = ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_AFTER_SEQUENCE_NUMBER = ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_LATEST = ShardIteratorType.LATEST.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_TRIM_HORIZON = ShardIteratorType.TRIM_HORIZON.toString();

    private static final String SHARD_ITERATOR_TYPE_KINESIS_AFTER_SEQUENCE_NUMBER = com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_AT_SEQUENCE_NUMBER = com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_LATEST = com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_TRIM_HORIZON = com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON.toString();

    private com.amazonaws.services.kinesis.model.GetShardIteratorRequest internalRequest;

    /**
     * Constructs a new request using an AmazonKinesis object.
     *
     * @param request Instance of AmazonKinesis GetShardIteratorRequest
     */
    public GetShardIteratorRequestAdapter(com.amazonaws.services.kinesis.model.GetShardIteratorRequest request) {
        internalRequest = request;
    }

    /**
     * @return The ARN of the stream.
     */
    @Override
    public String getStreamArn() {
        return internalRequest.getStreamName();
    }

    /**
     * @param streamArn The ARN of the stream.
     */
    @Override
    public void setStreamArn(String streamArn) {
        internalRequest.setStreamName(streamArn);
    }

    /**
     * @param streamArn The ARN of the stream.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withStreamArn(String streamArn) {
        internalRequest.withStreamName(streamArn);
        return this;
    }

    /**
     * @return The shard ID of the shard to get the iterator for.
     */
    @Override
    public String getShardId() {
        return internalRequest.getShardId();
    }

    /**
     * @param shardId The shard ID of the shard to get the iterator for.
     */
    @Override
    public void setShardId(String shardId) {
        internalRequest.setShardId(shardId);
    }

    /**
     * @param shardId The shard ID of the shard to get the iterator for.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withShardId(String shardId) {
        internalRequest.withShardId(shardId);
        return this;
    }

    /**
     * @return The sequence number of the data record in the shard from which to
     * start reading from.
     */
    @Override
    public String getSequenceNumber() {
        return internalRequest.getStartingSequenceNumber();
    }

    /**
     * @param sequenceNumber The sequence number of the data record in the shard from which to
     *                       start reading from.
     */
    @Override
    public void setSequenceNumber(String sequenceNumber) {
        internalRequest.setStartingSequenceNumber(sequenceNumber);
    }

    /**
     * @param sequenceNumber The sequence number of the data record in the shard from which to
     *                       start reading from.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withSequenceNumber(String sequenceNumber) {
        internalRequest.withStartingSequenceNumber(sequenceNumber);
        return this;
    }

    /**
     * @return Determines how the shard iterator is used to start reading data
     * records from the shard.
     */
    @Override
    public String getShardIteratorType() {
        return internalRequest.getShardIteratorType();
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *                          records from the shard.
     */
    @Override
    public void setShardIteratorType(String shardIteratorType) {
        if (SHARD_ITERATOR_TYPE_DYNAMODB_TRIM_HORIZON.equals(shardIteratorType)) {
            internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_TRIM_HORIZON);
        } else if (SHARD_ITERATOR_TYPE_DYNAMODB_LATEST.equals(shardIteratorType)) {
            internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_LATEST);
        } else if (SHARD_ITERATOR_TYPE_DYNAMODB_AT_SEQUENCE_NUMBER.equals(shardIteratorType)) {
            internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_AT_SEQUENCE_NUMBER);
        } else if (SHARD_ITERATOR_TYPE_DYNAMODB_AFTER_SEQUENCE_NUMBER.equals(shardIteratorType)) {
            internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_AFTER_SEQUENCE_NUMBER);
        } else {
            throw new IllegalArgumentException("Unsupported ShardIteratorType: " + shardIteratorType);
        }
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *                          records from the shard.
     */
    @Override
    public void setShardIteratorType(ShardIteratorType shardIteratorType) {
        setShardIteratorType(shardIteratorType.toString());
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *                          records from the shard.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withShardIteratorType(String shardIteratorType) {
        setShardIteratorType(shardIteratorType);
        return this;
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *                          records from the shard.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withShardIteratorType(ShardIteratorType shardIteratorType) {
        setShardIteratorType(shardIteratorType);
        return this;
    }

}
