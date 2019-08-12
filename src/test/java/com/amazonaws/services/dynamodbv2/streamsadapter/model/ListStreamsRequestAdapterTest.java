/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.kinesis.model.ListStreamsRequest;

public class ListStreamsRequestAdapterTest {
    private final String TEST_STRING = "TestString";

    private final Integer TEST_INT = 42;

    @Mock
    private ListStreamsRequest mockRequest;

    private ListStreamsRequestAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new ListStreamsRequestAdapter(mockRequest);
    }

    @Test
    public void testGetExclusiveStartStreamArn() {
        when(mockRequest.getExclusiveStartStreamName()).thenReturn(TEST_STRING);
        String actual = adapter.getExclusiveStartStreamArn();
        assertEquals(TEST_STRING, actual);
    }

    @Test
    public void testSetExclusiveStartStreamArn() {
        adapter.setExclusiveStartStreamArn(TEST_STRING);
        verify(mockRequest).setExclusiveStartStreamName(TEST_STRING);
    }

    @Test
    public void testWithExclusiveStartStreamArn() {
        Object actual = adapter.withExclusiveStartStreamArn(TEST_STRING);
        assertEquals(adapter, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetTableName() {
        adapter.getTableName();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetTableName() {
        adapter.setTableName(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithTableName() {
        adapter.withTableName(TEST_STRING);
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
        verify(mockRequest).setLimit(TEST_INT);
    }

    @Test
    public void testWithLimit() {
        Object actual = adapter.withLimit(TEST_INT);
        assertEquals(adapter, actual);
    }

    @Test
    public void testRealData() {
        ListStreamsRequest request = createRequest();
        ListStreamsRequestAdapter requestAdapter = new ListStreamsRequestAdapter(request);
        assertEquals(request.getExclusiveStartStreamName(), requestAdapter.getExclusiveStartStreamArn());
        assertEquals(request.getLimit(), requestAdapter.getLimit());
    }

    private ListStreamsRequest createRequest() {
        return new ListStreamsRequest().withExclusiveStartStreamName(TEST_STRING).withLimit(TEST_INT);
    }

}
