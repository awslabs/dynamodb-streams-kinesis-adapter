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

import com.amazonaws.services.kinesis.model.DescribeStreamRequest;

public class DescribeStreamRequestAdapterTest {
    private final String TEST_STRING = "TestString";
    private final Integer TEST_INT = 42;

    @Mock
    private DescribeStreamRequest mockRequest;

    private DescribeStreamRequestAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new DescribeStreamRequestAdapter(mockRequest);
    }

    @Test
    public void testGetExclusiveStartShardId() {
        when(mockRequest.getExclusiveStartShardId()).thenReturn(TEST_STRING);
        String actual = adapter.getExclusiveStartShardId();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetExclusiveStartShardId() {
        adapter.setExclusiveStartShardId(TEST_STRING);
        verify(mockRequest, times(1)).setExclusiveStartShardId(TEST_STRING);
    }

    @Test
    public void testWithExclusiveStartShardId() {
        Object actual = adapter.withExclusiveStartShardId(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test
    public void testGetLimit() {
        when(mockRequest.getLimit()).thenReturn(TEST_INT);
        Integer actual = adapter.getLimit();
        assertEquals(TEST_INT, actual);
    }

    @Test
    public void testSetLimit() {
        adapter.setLimit(TEST_INT);
        verify(mockRequest, times(1)).setLimit(TEST_INT);
    }

    @Test
    public void testWithLimit() {
        Object actual = adapter.withLimit(TEST_INT);
        assertEquals(adapter, actual);
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
    public void testRealData() {
        DescribeStreamRequest request = createRequest();
        DescribeStreamRequestAdapter requestAdapter = new DescribeStreamRequestAdapter(request);
        assertEquals(request.getExclusiveStartShardId(), requestAdapter.getExclusiveStartShardId());
        assertEquals(request.getLimit(), requestAdapter.getLimit());
        assertEquals(request.getStreamName(), requestAdapter.getStreamArn());
    }

    private DescribeStreamRequest createRequest() {
        return new DescribeStreamRequest().withExclusiveStartShardId(TEST_STRING).withLimit(TEST_INT).withStreamName(TEST_STRING);
    }

}
