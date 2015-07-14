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
    public java.util.List<String> getStreamNames() {
        List<com.amazonaws.services.dynamodbv2.model.Stream> streams = internalResult.getStreams();
        List<String> streamArns = new ArrayList<>(streams.size());
        for(com.amazonaws.services.dynamodbv2.model.Stream stream : streams) {
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
