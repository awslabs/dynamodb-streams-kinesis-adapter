/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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
    public String getExclusiveStartStreamArn() {
        return internalRequest.getExclusiveStartStreamName();
    }

    /**
     * @param exclusiveStartStreamArn The name of the stream to start the list with.
     */
    @Override
    public void setExclusiveStartStreamArn(String exclusiveStartStreamArn) {
        internalRequest.setExclusiveStartStreamName(exclusiveStartStreamArn);
    }

    /**
     * @param exclusiveStartStreamArn The name of the stream to start the list with.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    @Override
    public ListStreamsRequest withExclusiveStartStreamArn(String exclusiveStartStreamArn) {
        this.setExclusiveStartStreamArn(exclusiveStartStreamArn);
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
