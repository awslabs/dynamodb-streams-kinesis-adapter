/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;

public class ListStreamsResultAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private ListStreamsResult mockResult;

    private ListStreamsResultAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new ListStreamsResultAdapter(mockResult);
    }

    @Test
    public void testGetStreamNamesWithNoItems() {
        when(mockResult.getStreamIds()).thenReturn(new java.util.ArrayList<String>());
        java.util.List<String> actual = adapter.getStreamNames();
        assertTrue(actual.size() == 0);
    }

    @Test
    public void testGetStreamNamesWithItem() {
        java.util.List<String> streamList = new java.util.ArrayList<String>();
        streamList.add(TEST_STRING);
        when(mockResult.getStreamIds()).thenReturn(streamList);
        java.util.List<String> actual = adapter.getStreamNames();
        assertTrue(actual.size() == 1);
        assertEquals(TEST_STRING, actual.get(0));
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamNames() {
        adapter.setStreamNames(new java.util.ArrayList<String>());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithStreamNames() {
        adapter.withStreamNames(null, null);
    }

    @Test
    public void testGetHasMoreStreams() {
        when(mockResult.getLastEvaluatedStreamId()).thenReturn(null);
        Boolean result = adapter.getHasMoreStreams();
        assertFalse(result);
    }

    @Test
    public void testIsHasMoreStreams() {
        when(mockResult.getLastEvaluatedStreamId()).thenReturn(TEST_STRING);
        Boolean result = adapter.isHasMoreStreams();
        assertTrue(result);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetHasMoreStreams() {
        adapter.setHasMoreStreams(false);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithHasMoreStreams() {
        adapter.withHasMoreStreams(false);
    }

    @Test
    public void testRealDataNoIds() {
        ListStreamsResult result = createResult(false);
        ListStreamsResultAdapter resultAdapter = new ListStreamsResultAdapter(result);
        assertEquals(result.getStreamIds(), resultAdapter.getStreamNames());
    }

    @Test
    public void testRealDataWithIds() {
        ListStreamsResult result = createResult(true);
        ListStreamsResultAdapter resultAdapter = new ListStreamsResultAdapter(result);
        assertEquals(result.getStreamIds(), resultAdapter.getStreamNames());
    }

    private ListStreamsResult createResult(Boolean withIds) {
        java.util.List<String> streams = new java.util.ArrayList<String>();
        if(withIds) {
            streams.add(TEST_STRING);
        }
        return new ListStreamsResult()
            .withStreamIds(streams)
            .withLastEvaluatedStreamId(TEST_STRING);
    }

}
