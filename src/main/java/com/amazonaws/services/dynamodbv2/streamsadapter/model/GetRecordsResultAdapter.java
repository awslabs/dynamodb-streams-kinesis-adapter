/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Record;

/**
 * Represents the output of a GetRecords operation.
 */
public class GetRecordsResultAdapter extends GetRecordsResult {

    private com.amazonaws.services.dynamodbv2.model.GetRecordsResult internalResult;

    private java.util.List<Record> records;

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result Instance of DynamoDBStreams GetRecordsResult
     */
    public GetRecordsResultAdapter(com.amazonaws.services.dynamodbv2.model.GetRecordsResult result) {
        this(result, true);
    }

    /**
     * Constructs a new result using a DynamoDBStreams object.
     *
     * @param result                  Instance of DynamoDBStreams GetRecordsResult
     * @param generateRecordDataBytes Whether or not RecordAdapters should generate the ByteBuffer returned by getData().  KCL
     *                                uses the bytes returned by getData to generate throughput metrics.  If these metrics are not needed then
     *                                choosing to not generate this data results in memory and CPU savings.
     */
    public GetRecordsResultAdapter(com.amazonaws.services.dynamodbv2.model.GetRecordsResult result, boolean generateRecordDataBytes) {
        internalResult = result;
        records = new java.util.ArrayList<Record>();
        if (result.getRecords() != null) {
            for (com.amazonaws.services.dynamodbv2.model.Record record : result.getRecords()) {
                records.add(new RecordAdapter(record, generateRecordDataBytes));
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
     * reading data records. If set to <code>null</code>, the shard has been
     * closed and the requested iterator will not return any more data.
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
