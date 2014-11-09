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

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.StreamDescription;

/**
 * Represents the output of a DescribeStream operation.
 */
public class DescribeStreamResultAdapter extends DescribeStreamResult {

    private com.amazonaws.services.dynamodbv2.model.DescribeStreamResult internalResult;

    private StreamDescription streamDescription;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams DescribeStreamResult
     */
    public DescribeStreamResultAdapter(com.amazonaws.services.dynamodbv2.model.DescribeStreamResult result) {
        internalResult = result;
        streamDescription = new StreamDescriptionAdapter(result.getStreamDescription());
    }

    /**
     * @return Contains the current status of the stream, the stream ARN, an array of
     *         shard objects that comprise the stream, and states whether there are
     *         more shards available.
     */
    @Override
    public StreamDescription getStreamDescription() {
        return streamDescription;
    }

    @Override
    public void setStreamDescription(StreamDescription streamDescription) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DescribeStreamResult withStreamDescription(StreamDescription streamDescription) {
        throw new UnsupportedOperationException();
    }

}
