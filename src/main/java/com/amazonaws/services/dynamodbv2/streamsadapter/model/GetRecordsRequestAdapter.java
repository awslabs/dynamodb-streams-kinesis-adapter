/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;

/**
 * Container for the parameters to the GetRecords operation.
 */
public class GetRecordsRequestAdapter extends GetRecordsRequest {

    private com.amazonaws.services.kinesis.model.GetRecordsRequest internalRequest;

    /**
     * Constructs a new request from an Amazon Kinesis object.
     *
     * @param request Instance of AmazonKinesis GetRecordsReqest
     */
    public GetRecordsRequestAdapter(com.amazonaws.services.kinesis.model.GetRecordsRequest request) {
        internalRequest = request;
    }

    /**
     * @return The maximum number of records to return.
     */
    @Override
    public Integer getLimit() {
        return internalRequest.getLimit();
    }

    /**
     * @param limit The maximum number of records to return.
     */
    @Override
    public void setLimit(Integer limit) {
        internalRequest.setLimit(limit);
    }

    /**
     * @param limit The maximum number of records to return.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    @Override
    public GetRecordsRequest withLimit(Integer limit) {
        internalRequest.setLimit(limit);
        return this;
    }

    /**
     * @return The position in the shard from which you want to start sequentially
     * reading data records.
     */
    @Override
    public String getShardIterator() {
        return internalRequest.getShardIterator();
    }

    /**
     * @param shardIterator The position in the shard from which you want to start sequentially
     *                      reading data records.
     */
    @Override
    public void setShardIterator(String shardIterator) {
        internalRequest.setShardIterator(shardIterator);
    }

    /**
     * @param shardIterator The position in the shard from which you want to start sequentially
     *                      reading data records.
     * @return A reference to this updated object so that method calls can be chained together.
     */
    @Override
    public GetRecordsRequest withShardIterator(String shardIterator) {
        internalRequest.setShardIterator(shardIterator);
        return this;
    }

}
