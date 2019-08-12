/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A single update notification of a DynamoDB Stream, adapted for use
 * with the Amazon Kinesis model.
 * <p>
 * This class is designed to be used in a single thread only.
 */
public class RecordAdapter extends Record {

    private static Log LOG = LogFactory.getLog(RecordAdapter.class);

    public static final Charset defaultCharset = Charset.forName("UTF-8");

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private final com.amazonaws.services.dynamodbv2.model.Record internalRecord;

    private ByteBuffer data;

    private boolean generateDataBytes;

    /**
     * Constructs a new record using a DynamoDBStreams object.
     *
     * @param record Instance of DynamoDBStreams Record
     */
    public RecordAdapter(com.amazonaws.services.dynamodbv2.model.Record record) {
        this(record, true);
    }

    /**
     * Constructor for internal use
     *
     * @param record
     * @param generateDataBytes Whether or not to generate the ByteBuffer returned by getData().  KCL
     *                          uses the bytes returned by getData to generate throughput metrics.  If these metrics are not needed then
     *                          choosing to not generate this data results in memory and CPU savings.  If this value is true then
     *                          the data will be generated.  If false, getData() will return an empty ByteBuffer.
     * @throws IOException
     */
    RecordAdapter(com.amazonaws.services.dynamodbv2.model.Record record, boolean generateDataBytes) {
        internalRecord = record;
        this.generateDataBytes = generateDataBytes;
    }

    /**
     * @return The underlying DynamoDBStreams object
     */
    public com.amazonaws.services.dynamodbv2.model.Record getInternalObject() {
        return internalRecord;
    }

    /**
     * @return The unique identifier for the record in the DynamoDB stream.
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
     * This method returns JSON serialized {@link Record} object. However, This is not the best to use the object
     * It is recommended to get an object using {@link #getInternalObject()} and cast appropriately.
     *
     * @return JSON serialization of {@link Record} object. JSON contains only non-null
     * fields of {@link com.amazonaws.services.dynamodbv2.model.Record}. It returns null if serialization fails.
     */
    @Override
    public ByteBuffer getData() {
        if (data == null) {
            if (generateDataBytes) {
                try {
                    data = ByteBuffer.wrap(MAPPER.writeValueAsString(internalRecord).getBytes(defaultCharset));
                } catch (JsonProcessingException e) {
                    final String errorMessage = "Failed to serialize stream record to JSON";
                    LOG.error(errorMessage, e);
                    throw new RuntimeException(errorMessage, e);
                }
            } else {
                data = ByteBuffer.wrap(new byte[0]);
            }
        }
        return data;
    }

    @Override
    public void setData(ByteBuffer data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record withData(ByteBuffer data) {
        throw new UnsupportedOperationException();
    }

    /**
     * Jackson ObjectMapper requires a valid return value for serialization.
     */
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

    @Override
    public Date getApproximateArrivalTimestamp() {
        return internalRecord.getDynamodb().getApproximateCreationDateTime();
    }

    @Override
    public void setApproximateArrivalTimestamp(Date approximateArrivalTimestamp) {
        internalRecord.getDynamodb().setApproximateCreationDateTime(approximateArrivalTimestamp);
    }

    @Override
    public Record withApproximateArrivalTimestamp(Date approximateArrivalTimestamp) {
        setApproximateArrivalTimestamp(approximateArrivalTimestamp);
        return this;
    }

}
