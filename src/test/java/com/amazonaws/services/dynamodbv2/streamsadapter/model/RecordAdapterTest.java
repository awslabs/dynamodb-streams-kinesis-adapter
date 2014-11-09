package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RecordAdapterTest {
    private static final ObjectMapper MAPPER = new DynamoDBStreamsObjectMapper();

    private final String TEST_STRING = "TestString";

    private Record testRecord;

    private RecordAdapter adapter;

    @Before
    public void setUpTest() {
        testRecord = new Record();
        testRecord.setAwsRegion("us-east-1");
        testRecord.setEventID(UUID.randomUUID().toString());
        testRecord.setEventSource("aws:dynamodb");
        testRecord.setEventVersion("1.0");
        testRecord.setEventName(OperationType.MODIFY);
        StreamRecord testStreamRecord = new StreamRecord();
        testRecord.setDynamodb(testStreamRecord);
        Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("hashKey", new AttributeValue("hashKeyValue"));
        Map<String, AttributeValue> oldImage = new HashMap<String, AttributeValue>(key);
        Map<String, AttributeValue> newImage = new HashMap<String, AttributeValue>(key);
        newImage.put("newAttributeKey", new AttributeValue("someValue"));
        testStreamRecord.setKeys(key);
        testStreamRecord.setOldImage(oldImage);
        testStreamRecord.setNewImage(newImage);
        testStreamRecord.setSizeBytes(Long.MAX_VALUE);
        testStreamRecord.setSequenceNumber(UUID.randomUUID().toString());
        testStreamRecord.setStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        testStreamRecord.setSequenceNumber(TEST_STRING);
        try {
            adapter = new RecordAdapter(testRecord);
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetSequenceNumber() {
        String actual = adapter.getSequenceNumber();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetSequenceNumber() {
        adapter.setSequenceNumber(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithSequenceNumber() {
        adapter.withSequenceNumber(TEST_STRING);
    }

    @Test
    public void testGetData() {
        java.nio.ByteBuffer data = adapter.getData();
        Record actual = null;
        try {
            actual = MAPPER.readValue(data.array(), Record.class);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        assertEquals(testRecord, actual);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetData() {
        adapter.setData(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithData() {
        adapter.withData(null);
    }

    @Test
    public void testGetPartitionKey() {
        assertEquals(adapter.getPartitionKey(), null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetPartitionKey() {
        adapter.setPartitionKey(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithPartitionKey() {
        adapter.withPartitionKey(TEST_STRING);
    }

    @Test
    public void testGetInternalObject() {
        com.amazonaws.services.kinesis.model.Record kinesisRecord = null;
        try {
            kinesisRecord = new RecordAdapter(testRecord);
        } catch (IOException e) {
            fail(e.getMessage());
        }
        Record internalObject = ((RecordAdapter) kinesisRecord).getInternalObject();
        assertEquals(testRecord, internalObject);
    }

}
