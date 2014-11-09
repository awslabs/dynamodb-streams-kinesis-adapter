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
import java.nio.charset.Charset;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A single update notification of a DynamoDB Stream, adapted for use
 * with the Amazon Kinesis model.
 */
public class RecordAdapter extends Record {

    public static final String charSetName = "UTF-8";

    private static final ObjectMapper MAPPER = new DynamoDBStreamsObjectMapper();

    private com.amazonaws.services.dynamodbv2.model.Record internalRecord;

    private java.nio.ByteBuffer data;

    /**
     * Constructs a new record using a Streams object.
     *
     * @param record Instance of Streams Record
     */
    public RecordAdapter(com.amazonaws.services.dynamodbv2.model.Record record) throws IOException {
        internalRecord = record;
        serializeData();
    }

    private void serializeData() throws IOException {
        String json = MAPPER.writeValueAsString(internalRecord);
        Charset utf8 = Charset.forName(charSetName);
        data = java.nio.ByteBuffer.wrap(json.getBytes(utf8));
    }

    /**
     * @return The underlying Streams object
     */
    public com.amazonaws.services.dynamodbv2.model.Record getInternalObject() {
        return internalRecord;
    }

    /**
     * @return The unique identifier for the record in the DynamoDB Stream.
     */
    @Override
    public String getSequenceNumber() {
        return internalRecord.getDynamodb().getSequenceNumber();
    }

    @Override
    public void setSequenceNumber(String sequenceNumber) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record withSequenceNumber(String sequenceNumber) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return The data blob. Contains the update view type, type of the operation
     *          performed, key set, and optionally the old and new attribute values.
     */
    @Override
    public java.nio.ByteBuffer getData() {
        return data;
    }

    @Override
    public void setData(java.nio.ByteBuffer data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record withData(java.nio.ByteBuffer data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getPartitionKey() {
        return null;
    }

    @Override
    public void setPartitionKey(String partitionKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record withPartitionKey(String partitionKey) {
        throw new UnsupportedOperationException();
    }

}
