/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.StreamDescription;

/**
 * Represents the output of a DescribeStream operation.
 */
public class DescribeStreamResultAdapter extends DescribeStreamResult {

    private StreamDescription streamDescription;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams DescribeStreamResult
     */
    public DescribeStreamResultAdapter(com.amazonaws.services.dynamodbv2.model.DescribeStreamResult result) {
        streamDescription = new StreamDescriptionAdapter(result.getStreamDescription());
    }

    /**
     * @return Contains the current status of the stream, the stream ARN, an array of
     * shard objects that comprise the stream, and states whether there are
     * more shards available.
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
