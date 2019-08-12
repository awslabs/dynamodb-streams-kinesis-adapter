/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@PrepareForTest({ObjectMapper.class, RecordAdapter.class})
@RunWith(PowerMockRunner.class)
public class RecordAdapterTest {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final ObjectMapper MOCK_MAPPER = mock(RecordObjectMapper.class);

    private static final String TEST_STRING = "TestString";

    private static final Date TEST_DATE = new Date(1156377600 /* EC2 Announced */);

    private static final String TEST_RECORD_v1_0 =
        new StringBuilder().append("{").append("\"awsRegion\":\"us-east-1\",").append("\"dynamodb\":").append("{").append("\"Keys\":").append("{")
            .append("\"hashKey\":{\"S\":\"hashKeyValue\"}").append("},").append("\"StreamViewType\":\"NEW_AND_OLD_IMAGES\",")
            .append("\"SequenceNumber\":\"100000000003498069978\",").append("\"SizeBytes\":6").append("},").append("\"eventID\":\"33fe21d365c03362c5e66d8dec2b63d5\",")
            .append("\"eventVersion\":\"1.0\",").append("\"eventName\":\"INSERT\",").append("\"eventSource\":\"aws:dynamodb\"").append("}").toString();

    private Record testRecord;

    private RecordAdapter adapter;

    @Before
    public void setUpTest() {
        testRecord = new Record();
        testRecord.setAwsRegion("us-east-1");
        testRecord.setEventID(UUID.randomUUID().toString());
        testRecord.setEventSource("aws:dynamodb");
        testRecord.setEventVersion("1.1");
        testRecord.setEventName(OperationType.MODIFY);
        StreamRecord testStreamRecord = new StreamRecord();
        testRecord.setDynamodb(testStreamRecord);
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", new AttributeValue("hashKeyValue"));
        Map<String, AttributeValue> oldImage = new HashMap<String, AttributeValue>(key);
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", new AttributeValue("someValue"));
        testStreamRecord.setApproximateCreationDateTime(TEST_DATE);
        testStreamRecord.setKeys(key);
        testStreamRecord.setOldImage(oldImage);
        testStreamRecord.setNewImage(newImage);
        testStreamRecord.setSizeBytes(Long.MAX_VALUE);
        testStreamRecord.setSequenceNumber(UUID.randomUUID().toString());
        testStreamRecord.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        testStreamRecord.setSequenceNumber(TEST_STRING);
        adapter = new RecordAdapter(testRecord);
    }

    @Test
    public void testDoesNotGenerateBytesWhenGenerateDataBytesIsFalse() {
        adapter = new RecordAdapter(testRecord, false);
        assertEquals(0, adapter.getData().array().length);
    }

    @Test
    public void testGetSequenceNumber() {
        String actual = adapter.getSequenceNumber();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSequenceNumber() {
        adapter.setSequenceNumber(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithSequenceNumber() {
        adapter.withSequenceNumber(TEST_STRING);
    }

    @Test
    public void testGetData() throws JsonProcessingException {
        Whitebox.setInternalState(RecordAdapter.class, ObjectMapper.class, MOCK_MAPPER);
        when(MOCK_MAPPER.writeValueAsString(adapter.getInternalObject())).thenReturn(MAPPER.writeValueAsString(adapter.getInternalObject()));
        ByteBuffer data = ByteBuffer.wrap(MAPPER.writeValueAsString(adapter.getInternalObject()).getBytes());
        assertEquals(data, adapter.getData());
        // Retrieve data twice to validate it is only deserialized once
        assertEquals(data, adapter.getData());
        verify(MOCK_MAPPER, times(1)).writeValueAsString(adapter.getInternalObject());
    }

    @Test(expected = RuntimeException.class)
    public void testGetDataMappingException() throws JsonProcessingException {
        Whitebox.setInternalState(RecordAdapter.class, ObjectMapper.class, MOCK_MAPPER);
        when(MOCK_MAPPER.writeValueAsString(adapter.getInternalObject())).thenThrow(mock(JsonProcessingException.class));
        adapter.getData();
    }

    /**
     * We need a custom serializer/deserializer to be able to process Record object because of the conflicts that arise
     * with the standard jackson mapper for fields like eventName etc.
     */
    @Test
    public void testGetDataDeserialized() throws JsonParseException, JsonMappingException, IOException {
        Whitebox.setInternalState(RecordAdapter.class, ObjectMapper.class, MAPPER);

        java.nio.ByteBuffer data = adapter.getData();
        Record actual = MAPPER.readValue(data.array(), Record.class);
        assertEquals(adapter.getInternalObject(), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetData() {
        adapter.setData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithData() {
        adapter.withData(null);
    }

    @Test
    public void testGetPartitionKey() {
        assertEquals(adapter.getPartitionKey(), null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetPartitionKey() {
        adapter.setPartitionKey(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithPartitionKey() {
        adapter.withPartitionKey(TEST_STRING);
    }

    @Test
    public void testApproximateCreationDateTime() throws IOException {
        String serialized = MAPPER.writeValueAsString(testRecord);
        Record deserialized = MAPPER.readValue(serialized, Record.class);
        com.amazonaws.services.kinesis.model.Record adapter = new RecordAdapter(deserialized);
        assertEquals(TEST_DATE, adapter.getApproximateArrivalTimestamp());
        Date newDate = new Date();
        adapter.setApproximateArrivalTimestamp(newDate);
        assertEquals(newDate, deserialized.getDynamodb().getApproximateCreationDateTime());
        assertEquals(newDate, adapter.getApproximateArrivalTimestamp());
        adapter.withApproximateArrivalTimestamp(TEST_DATE);
        assertEquals(TEST_DATE, deserialized.getDynamodb().getApproximateCreationDateTime());
        assertEquals(TEST_DATE, adapter.getApproximateArrivalTimestamp());
    }

    @Test
    public void testGetInternalObject() {
        com.amazonaws.services.kinesis.model.Record kinesisRecord = null;
        kinesisRecord = new RecordAdapter(testRecord);
        Record internalObject = ((RecordAdapter) kinesisRecord).getInternalObject();
        assertEquals(testRecord, internalObject);
    }

    @Test
    public void testRecord_v1_0() throws IOException {
        Record deserialized = MAPPER.readValue(TEST_RECORD_v1_0, Record.class);
        com.amazonaws.services.kinesis.model.Record adapter = new RecordAdapter(deserialized);
        assertNull(adapter.getApproximateArrivalTimestamp());
    }

}
