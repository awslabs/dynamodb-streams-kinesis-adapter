/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;

public class DescribeStreamResultAdapterTest {

    @Mock
    private DescribeStreamResult mockResult;

    @Mock
    private StreamDescription mockDescription;

    private DescribeStreamResultAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        when(mockResult.getStreamDescription()).thenReturn(mockDescription);
        adapter = new DescribeStreamResultAdapter(mockResult);
    }

    @Test
    public void testGetStreamDescription() {
        Object streamDescription = adapter.getStreamDescription();
        assertTrue(streamDescription instanceof StreamDescriptionAdapter);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamDescription() {
        adapter.setStreamDescription(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamDescription() {
        adapter.withStreamDescription(null);
    }

}
