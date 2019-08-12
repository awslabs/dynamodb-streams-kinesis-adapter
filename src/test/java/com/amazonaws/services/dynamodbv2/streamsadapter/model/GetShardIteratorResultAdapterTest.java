/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;

public class GetShardIteratorResultAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private GetShardIteratorResult mockResult;

    private GetShardIteratorResultAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new GetShardIteratorResultAdapter(mockResult);
    }

    @Test
    public void testGetShardIterator() {
        when(mockResult.getShardIterator()).thenReturn(TEST_STRING);
        String actual = adapter.getShardIterator();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShardIterator() {
        adapter.setShardIterator(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithShardIterator() {
        adapter.withShardIterator(TEST_STRING);
    }

    @Test
    public void testRealData() {
        GetShardIteratorResult result = createResult();
        GetShardIteratorResultAdapter resultAdapter = new GetShardIteratorResultAdapter(result);
        assertEquals(result.getShardIterator(), resultAdapter.getShardIterator());
    }

    private GetShardIteratorResult createResult() {
        return new GetShardIteratorResult().withShardIterator(TEST_STRING);
    }

}
