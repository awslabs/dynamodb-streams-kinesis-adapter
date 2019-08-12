/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.kinesis.model.GetShardIteratorResult;

/**
 * Represents the output of a GetShardIterator operation.
 */
public class GetShardIteratorResultAdapter extends GetShardIteratorResult {

    private com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult internalResult;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams GetShardIteratorResult
     */
    public GetShardIteratorResultAdapter(com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult result) {
        internalResult = result;
    }

    /**
     * @return The position in the shard from which to start reading data records
     * sequentially.
     */
    @Override
    public String getShardIterator() {
        return internalResult.getShardIterator();
    }

    @Override
    public void setShardIterator(String shardIterator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetShardIteratorResult withShardIterator(String shardIterator) {
        throw new UnsupportedOperationException();
    }

}
