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

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.nio.ByteBuffer;

import java.io.IOException;
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

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@PrepareForTest({ObjectMapper.class, RecordAdapter.class})
@RunWith(PowerMockRunner.class)
public class RecordAdapterTest {

    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final ObjectMapper MOCK_MAPPER = mock(RecordObjectMapper.class);

    private static final String TEST_STRING = "TestString";

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
        when(MOCK_MAPPER.writeValueAsString(adapter.getInternalObject())).thenReturn(
            MAPPER.writeValueAsString(adapter.getInternalObject()));
        ByteBuffer data = ByteBuffer.wrap(MAPPER.writeValueAsString(adapter.getInternalObject()).getBytes());
        assertEquals(data, adapter.getData());
        // Retrieve data twice to validate it is only deserialized once
        assertEquals(data, adapter.getData());
        verify(MOCK_MAPPER, times(1)).writeValueAsString(adapter.getInternalObject());
    }

    @Test(expected = RuntimeException.class)
    public void testGetDataMappingException() throws JsonProcessingException {
        Whitebox.setInternalState(RecordAdapter.class, ObjectMapper.class, MOCK_MAPPER);
        when(MOCK_MAPPER.writeValueAsString(adapter.getInternalObject()))
            .thenThrow(mock(JsonProcessingException.class));
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
    public void testGetInternalObject() {
        com.amazonaws.services.kinesis.model.Record kinesisRecord = null;
        kinesisRecord = new RecordAdapter(testRecord);
        Record internalObject = ((RecordAdapter) kinesisRecord).getInternalObject();
        assertEquals(testRecord, internalObject);
    }

}
