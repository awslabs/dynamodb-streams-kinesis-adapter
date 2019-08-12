/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.model.ListStreamsResult;

/**
 * Represents the output of a ListStreams operation.
 */
public class ListStreamsResultAdapter extends ListStreamsResult {

    private com.amazonaws.services.dynamodbv2.model.ListStreamsResult internalResult;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams ListStreamsResult
     */
    public ListStreamsResultAdapter(com.amazonaws.services.dynamodbv2.model.ListStreamsResult result) {
        internalResult = result;
    }

    /**
     * The names of the streams that are associated with the AWS account
     * making the request.
     */
    @Override
    public List<String> getStreamNames() {
        List<com.amazonaws.services.dynamodbv2.model.Stream> streams = internalResult.getStreams();
        List<String> streamArns = new ArrayList<>(streams.size());
        for (com.amazonaws.services.dynamodbv2.model.Stream stream : streams) {
            streamArns.add(stream.getStreamArn());
        }
        return streamArns;
    }

    @Override
    public void setStreamNames(java.util.Collection<String> streamNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListStreamsResult withStreamNames(java.util.Collection<String> streamNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListStreamsResult withStreamNames(String... streamNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return If true, there are more streams available to list.
     */
    @Override
    public Boolean isHasMoreStreams() {
        return internalResult.getLastEvaluatedStreamArn() != null;
    }

    /**
     * @return If true, there are more streams available to list.
     */
    @Override
    public Boolean getHasMoreStreams() {
        return internalResult.getLastEvaluatedStreamArn() != null;
    }

    @Override
    public void setHasMoreStreams(Boolean hasMoreStreams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListStreamsResult withHasMoreStreams(Boolean hasMoreStreams) {
        throw new UnsupportedOperationException();
    }

}
