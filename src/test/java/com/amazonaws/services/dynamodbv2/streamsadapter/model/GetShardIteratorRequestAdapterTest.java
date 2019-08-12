/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;

public class GetShardIteratorRequestAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private GetShardIteratorRequest mockRequest;

    private GetShardIteratorRequestAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new GetShardIteratorRequestAdapter(mockRequest);
    }

    @Test
    public void testGetStreamArn() {
        when(mockRequest.getStreamName()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamArn();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetStreamArn() {
        adapter.setStreamArn(TEST_STRING);
        verify(mockRequest, times(1)).setStreamName(TEST_STRING);
    }

    @Test
    public void testWithStreamArn() {
        Object actual = adapter.withStreamArn(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetShardId() {
        when(mockRequest.getShardId()).thenReturn(TEST_STRING);
        String actual = adapter.getShardId();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetShardId() {
        adapter.setShardId(TEST_STRING);
        verify(mockRequest, times(1)).setShardId(TEST_STRING);
    }

    @Test
    public void testWithShardId() {
        Object actual = adapter.withShardId(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetSequenceNumber() {
        when(mockRequest.getStartingSequenceNumber()).thenReturn(TEST_STRING);
        String actual = adapter.getSequenceNumber();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetSequenceNumber() {
        adapter.setSequenceNumber(TEST_STRING);
        verify(mockRequest, times(1)).setStartingSequenceNumber(TEST_STRING);
    }

    @Test
    public void testWithSequenceNumber() {
        Object actual = adapter.withSequenceNumber(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetShardIteratorType() {
        when(mockRequest.getShardIteratorType()).thenReturn(TEST_STRING);
        String actual = adapter.getShardIteratorType();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetShardIteratorTypeFailure() {
        adapter.setShardIteratorType(TEST_STRING);
    }

    @Test
    public void testSetShardIteratorTypeAsTypeAfterSequenceNumber() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsTypeAtSequenceNumber() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER);
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsTypeLatest() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST);
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsTypeTrimHorizon() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON);
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithShardIteratorTypeFailure() {
        adapter.withShardIteratorType(TEST_STRING);
    }

    @Test
    public void testWithShardIteratorTypeAsStringAfterSequenceNumber() {
        adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testWithShardIteratorTypeAsStringAtSequenceNumber() {
        adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testWithShardIteratorTypeAsStringLatest() {
        adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST.toString());
    }

    @Test
    public void testWithShardIteratorTypeAsStringTrimHorizon() {
        adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsStringAfterSequenceNumber() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsStringAtSequenceNumber() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsStringLatest() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST.toString());
    }

    @Test
    public void testSetShardIteratorTypeAsStringTrimHorizon() {
        adapter.setShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON.toString());
        verify(mockRequest, times(1)).setShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON.toString());
    }

    @Test
    public void testWithShardIteratorTypeAsType() {
        Object actual = adapter.withShardIteratorType(com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST);
        assertEquals(adapter, actual);
    }

    @Test
    public void testRealData() {
        GetShardIteratorRequest request = createRequest();
        GetShardIteratorRequestAdapter requestAdapter = new GetShardIteratorRequestAdapter(request);
        assertEquals(request.getStartingSequenceNumber(), requestAdapter.getSequenceNumber());
        assertEquals(request.getShardId(), requestAdapter.getShardId());
        assertEquals(request.getShardIteratorType(), requestAdapter.getShardIteratorType());
        assertEquals(request.getStreamName(), requestAdapter.getStreamArn());
    }

    private GetShardIteratorRequest createRequest() {
        return new GetShardIteratorRequest()
            .withShardId(TEST_STRING)
            .withStartingSequenceNumber(TEST_STRING)
            .withShardIteratorType(com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST)
            .withStreamName(TEST_STRING);
    }

}
