/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;

/**
 * Container for all information describing a single DynamoDB Stream.
 */
public class StreamDescriptionAdapter extends StreamDescription {
    // Evaluate each StreamStatus.toString() only once
    private static final String STREAM_STATUS_DYNAMODB_DISABLED = com.amazonaws.services.dynamodbv2.model.StreamStatus.DISABLED.toString();
    private static final String STREAM_STATUS_DYNAMODB_DISABLING = com.amazonaws.services.dynamodbv2.model.StreamStatus.DISABLING.toString();
    private static final String STREAM_STATUS_DYNAMODB_ENABLED = com.amazonaws.services.dynamodbv2.model.StreamStatus.ENABLED.toString();
    private static final String STREAM_STATUS_DYNAMODB_ENABLING = com.amazonaws.services.dynamodbv2.model.StreamStatus.ENABLING.toString();
    private static final String STREAM_STATUS_KINESIS_ACTIVE = StreamStatus.ACTIVE.toString();
    private static final String STREAM_STATUS_KINESIS_CREATING = StreamStatus.CREATING.toString();

    private final com.amazonaws.services.dynamodbv2.model.StreamDescription internalDescription;

    private final List<Shard> shards;

    /**
     * Constructs a new description using a DynamoDBStreams object.
     *
     * @param streamDescription Instance of DynamoDBStreams StreamDescription
     */
    public StreamDescriptionAdapter(com.amazonaws.services.dynamodbv2.model.StreamDescription streamDescription) {
        internalDescription = streamDescription;
        shards = new ArrayList<Shard>();
        for (com.amazonaws.services.dynamodbv2.model.Shard shard : streamDescription.getShards()) {
            shards.add(new ShardAdapter(shard));
        }
    }

    /**
     * @return The underlying DynamoDBStreams object
     */
    public com.amazonaws.services.dynamodbv2.model.StreamDescription getInternalObject() {
        return internalDescription;
    }

    /**
     * @return The name of the stream being described.
     */
    @Override
    public String getStreamName() {
        return internalDescription.getStreamArn();
    }

    @Override
    public void setStreamName(String streamName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withStreamName(String streamName) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The Amazon Resource Name (ARN) for the stream being described.
     */
    @Override
    public String getStreamARN() {
        return internalDescription.getStreamArn();
    }

    @Override
    public void setStreamARN(String streamARN) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withStreamARN(String streamARN) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The current status of the stream being described.
     */
    @Override
    public String getStreamStatus() {
        String status = internalDescription.getStreamStatus();
        if (STREAM_STATUS_DYNAMODB_ENABLED.equals(status)) {
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else if (STREAM_STATUS_DYNAMODB_ENABLING.equals(status)) {
            status = STREAM_STATUS_KINESIS_CREATING;
        } else if (STREAM_STATUS_DYNAMODB_DISABLED.equals(status)) {
            // streams are valid for 24hrs after disabling and
            // will continue to support read operations
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else if (STREAM_STATUS_DYNAMODB_DISABLING.equals(status)) {
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else {
            throw new UnsupportedOperationException("Unsupported StreamStatus: " + status);
        }
        return status;
    }

    @Override
    public void setStreamStatus(String streamStatus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withStreamStatus(String streamStatus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStreamStatus(StreamStatus streamStatus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withStreamStatus(StreamStatus streamStatus) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The shards that comprise the stream.
     */
    @Override
    public List<Shard> getShards() {
        return shards;
    }

    @Override
    public void setShards(java.util.Collection<Shard> shards) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withShards(Shard... shards) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withShards(java.util.Collection<Shard> shards) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return If true there are more shards in the stream
     * available to describe.
     */
    @Override
    public Boolean isHasMoreShards() {
        return internalDescription.getLastEvaluatedShardId() != null;
    }

    /**
     * @return If true there are more shards in the stream
     * available to describe.
     */
    @Override
    public Boolean getHasMoreShards() {
        return internalDescription.getLastEvaluatedShardId() != null;
    }

    @Override
    public void setHasMoreShards(Boolean hasMoreShards) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamDescription withHasMoreShards(Boolean hasMoreShards) {
        throw new UnsupportedOperationException();
    }

}
