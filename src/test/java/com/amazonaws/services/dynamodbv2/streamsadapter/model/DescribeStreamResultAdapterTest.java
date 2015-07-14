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

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamDescription() {
        adapter.setStreamDescription(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithStreamDescription() {
        adapter.withStreamDescription(null);
    }

}
