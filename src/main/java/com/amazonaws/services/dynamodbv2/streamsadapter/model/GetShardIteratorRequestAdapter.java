/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;

/**
 * Container for the parameters to the GetShardIterator operation.
 */
public class GetShardIteratorRequestAdapter extends GetShardIteratorRequest {

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
     * @return The ID of the stream.
     */
    @Override
    public String getStreamId() {
        return internalRequest.getStreamName();
    }

    /**
     * @param streamId The ID of the stream.
     */
    @Override
    public void setStreamId(String streamId) {
        internalRequest.setStreamName(streamId);
    }

    /**
     * @param streamId The ID of the stream.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withStreamId(String streamId) {
        internalRequest.withStreamName(streamId);
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
     *         start reading from.
     */
    @Override
    public String getSequenceNumber() {
        return internalRequest.getStartingSequenceNumber();
    }

    /**
     * @param sequenceNumber The sequence number of the data record in the shard from which to
     *         start reading from.
     */
    @Override
    public void setSequenceNumber(String sequenceNumber) {
        internalRequest.setStartingSequenceNumber(sequenceNumber);
    }

    /**
     * @param sequenceNumber The sequence number of the data record in the shard from which to
     *         start reading from.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withSequenceNumber(String sequenceNumber) {
        internalRequest.withStartingSequenceNumber(sequenceNumber);
        return this;
    }

    /**
     * @return Determines how the shard iterator is used to start reading data
     *         records from the shard.
     */
    @Override
    public String getShardIteratorType() {
        return internalRequest.getShardIteratorType();
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *         records from the shard.
     */
    @Override
    public void setShardIteratorType(String shardIteratorType) {
        this.setShardIteratorType(ShardIteratorType.fromValue(shardIteratorType));
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *         records from the shard.
     */
    @Override
    public void setShardIteratorType(ShardIteratorType shardIteratorType) {
        switch(shardIteratorType) {
        case TRIM_HORIZON:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON);
            break;
        case LATEST:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST);
            break;
        case AT_SEQUENCE_NUMBER:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER);
            break;
        case AFTER_SEQUENCE_NUMBER:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        }
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *         records from the shard.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withShardIteratorType(String shardIteratorType) {
        return this.withShardIteratorType(ShardIteratorType.fromValue(shardIteratorType));
    }

    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *         records from the shard.
     * @return Returns a reference to this object so that method calls can be chained together.
     */
    @Override
    public GetShardIteratorRequest withShardIteratorType(ShardIteratorType shardIteratorType) {
        switch(shardIteratorType) {
        case TRIM_HORIZON:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON);
            break;
        case LATEST:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST);
            break;
        case AT_SEQUENCE_NUMBER:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER);
            break;
        case AFTER_SEQUENCE_NUMBER:
            internalRequest.setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        }
        return this;
    }

}
