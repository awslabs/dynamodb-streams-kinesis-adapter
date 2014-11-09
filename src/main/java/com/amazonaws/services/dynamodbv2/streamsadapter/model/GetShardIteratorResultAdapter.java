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
     *         sequentially.
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
