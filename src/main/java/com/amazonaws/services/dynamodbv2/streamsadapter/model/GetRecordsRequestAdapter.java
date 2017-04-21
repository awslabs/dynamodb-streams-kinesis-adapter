/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
