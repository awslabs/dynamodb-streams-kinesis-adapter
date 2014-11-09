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

import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;

/**
 * Container for the parameters to the ListStreams operation.
 */
public class ListStreamsRequestAdapter extends ListStreamsRequest {

    private com.amazonaws.services.kinesis.model.ListStreamsRequest internalRequest;

    /**
     * Constructs a new request using an AmazonKinesis object.
     *
     * @param request Instance of AmazonKinesis ListStreamsRequest
     */
    public ListStreamsRequestAdapter(com.amazonaws.services.kinesis.model.ListStreamsRequest request) {
        internalRequest = request;
    }

    /**
     * @return The name of the stream to start the list with.
     */
    @Override
    public String getExclusiveStartStreamId() {
        return internalRequest.getExclusiveStartStreamName();
    }

    /**
     * @param exclusiveStartStreamId The name of the stream to start the list with.
     */
    @Override
    public void setExclusiveStartStreamId(String exclusiveStartStreamId) {
        internalRequest.setExclusiveStartStreamName(exclusiveStartStreamId);
    }

    /**
     * @param exclusiveStartStreamId The name of the stream to start the list with.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    @Override
    public ListStreamsRequest withExclusiveStartStreamId(String exclusiveStartStreamId) {
        this.setExclusiveStartStreamId(exclusiveStartStreamId);
        return this;
    }

    /**
     * @return The maximum number of streams to list.
     */
    @Override
    public Integer getLimit() {
        return internalRequest.getLimit();
    }

    /**
     * @param limit The maximum number of streams to list.
     */
    @Override
    public void setLimit(Integer limit) {
        internalRequest.setLimit(limit);
    }

    /**
     * @param limit The maximum number of streams to list.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    @Override
    public ListStreamsRequest withLimit(Integer limit) {
        this.setLimit(limit);
        return this;
    }

    // Not supported by the underlying Kinesis class
    @Override
    public String getTableName() {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Kinesis class
    @Override
    public void setTableName(String tableName) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Kinesis class
    @Override
    public ListStreamsRequest withTableName(String tableName) {
        throw new UnsupportedOperationException();
    }

}
