/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Stream;

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
        when(mockResult.getStreams()).thenReturn(new java.util.ArrayList<Stream>());
        java.util.List<String> actual = adapter.getStreamNames();
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testGetStreamNamesWithItem() {
        java.util.List<Stream> streamList = new java.util.ArrayList<>();
        Stream stream = new Stream();
        stream.setStreamArn(TEST_STRING);
        streamList.add(stream);
        when(mockResult.getStreams()).thenReturn(streamList);

        java.util.List<String> actual = adapter.getStreamNames();
        assertTrue(actual.size() == 1);
        assertEquals(TEST_STRING, actual.get(0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamNames() {
        adapter.setStreamNames(new java.util.ArrayList<String>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamNames() {
        adapter.withStreamNames(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamNames2() {
        final Collection<String> streamNames = Collections.emptyList();
        adapter.withStreamNames(streamNames);
    }

    @Test
    public void testGetHasMoreStreamsTrue() {
        when(mockResult.getLastEvaluatedStreamArn()).thenReturn(TEST_STRING);
        assertTrue(adapter.getHasMoreStreams());
    }

    @Test
    public void testGetHasMoreStreamsFalse() {
        when(mockResult.getLastEvaluatedStreamArn()).thenReturn(null);
        assertFalse(adapter.getHasMoreStreams());
    }

    @Test
    public void testIsHasMoreStreamsTrue() {
        when(mockResult.getLastEvaluatedStreamArn()).thenReturn(TEST_STRING);
        assertTrue(adapter.isHasMoreStreams());
    }

    @Test
    public void testIsHasMoreStreamsFalse() {
        when(mockResult.getLastEvaluatedStreamArn()).thenReturn(null);
        assertFalse(adapter.isHasMoreStreams());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetHasMoreStreams() {
        adapter.setHasMoreStreams(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithHasMoreStreams() {
        adapter.withHasMoreStreams(false);
    }

    @Test
    public void testRealDataNoIds() {
        ListStreamsResult result = createResult(false);
        ListStreamsResultAdapter resultAdapter = new ListStreamsResultAdapter(result);
        List<String> streamArns = extractStreamArns(result);
        assertEquals(streamArns, resultAdapter.getStreamNames());
    }

    @Test
    public void testRealDataWithIds() {
        ListStreamsResult result = createResult(true);
        ListStreamsResultAdapter resultAdapter = new ListStreamsResultAdapter(result);
        assertEquals(extractStreamArns(result), resultAdapter.getStreamNames());
    }

    private List<String> extractStreamArns(ListStreamsResult result) {
        List<Stream> streams = result.getStreams();
        List<String> streamArns = new ArrayList<>(streams.size());
        for (Stream stream : streams) {
            streamArns.add(stream.getStreamArn());
        }
        return streamArns;
    }

    private ListStreamsResult createResult(Boolean withArns) {
        java.util.List<Stream> streams = new java.util.ArrayList<>();
        if (withArns) {
            Stream stream = new Stream();
            stream.setStreamArn(TEST_STRING);
            streams.add(stream);
        }
        return new ListStreamsResult().withStreams(streams).withLastEvaluatedStreamArn(TEST_STRING);
    }

}
