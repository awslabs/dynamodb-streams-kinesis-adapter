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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Represents the output of a GetRecords operation.
 */
public class GetRecordsResultAdapter extends GetRecordsResult {

    private static Logger LOGGER = Logger.getLogger(GetRecordsResultAdapter.class.getName());

    private com.amazonaws.services.dynamodbv2.model.GetRecordsResult internalResult;

    private java.util.List<Record> records;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams GetRecordsResult
     */
    public GetRecordsResultAdapter(com.amazonaws.services.dynamodbv2.model.GetRecordsResult result) {
        internalResult = result;
        records = new java.util.ArrayList<Record>();
        if (result.getRecords() != null) {
            for(com.amazonaws.services.dynamodbv2.model.Record record : result.getRecords()) {
                try {
                    records.add(new RecordAdapter(record));
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Could not process record", e);
                }
            }
        }
    }

    /**
     * @return The data records retrieved from the shard
     */
    @Override
    public java.util.List<Record> getRecords() {
        return records;
    }

    @Override
    public void setRecords(java.util.Collection<Record> records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetRecordsResult withRecords(Record... records) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetRecordsResult withRecords(java.util.Collection<Record> records) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The next position in the shard from which to start sequentially
     *         reading data records. If set to <code>null</code>, the shard has been
     *         closed and the requested iterator will not return any more data.
     */
    @Override
    public String getNextShardIterator() {
        return internalResult.getNextShardIterator();
    }

    @Override
    public void setNextShardIterator(String nextShardIterator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetRecordsResult withNextShardIterator(String nextShardIterator) {
        throw new UnsupportedOperationException();
    }

}
