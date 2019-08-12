/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;

public class StreamDescriptionAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private StreamDescription mockDescription;

    private StreamDescriptionAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        java.util.List<Shard> shards = new java.util.ArrayList<Shard>();
        shards.add(new Shard());
        when(mockDescription.getShards()).thenReturn(shards);
        adapter = new StreamDescriptionAdapter(mockDescription);
    }

    @Test
    public void testGetStreamName() {
        when(mockDescription.getStreamArn()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamName();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamName() {
        adapter.setStreamName(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamName() {
        adapter.withStreamName(TEST_STRING);
    }

    @Test
    public void testGetStreamARN() {
        when(mockDescription.getStreamArn()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamARN();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamARN() {
        adapter.setStreamARN(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamARN() {
        adapter.withStreamARN(TEST_STRING);
    }

    @Test
    public void testGetStreamStatus() {
        when(mockDescription.getStreamStatus()).thenReturn(StreamStatus.ENABLING.toString());
        String actual = adapter.getStreamStatus();
        assertEquals(com.amazonaws.services.kinesis.model.StreamStatus.CREATING.toString(), actual);

        when(mockDescription.getStreamStatus()).thenReturn(StreamStatus.ENABLED.toString());
        actual = adapter.getStreamStatus();
        assertEquals(com.amazonaws.services.kinesis.model.StreamStatus.ACTIVE.toString(), actual);

        when(mockDescription.getStreamStatus()).thenReturn(StreamStatus.DISABLING.toString());
        actual = adapter.getStreamStatus();
        assertEquals(com.amazonaws.services.kinesis.model.StreamStatus.ACTIVE.toString(), actual);

        when(mockDescription.getStreamStatus()).thenReturn(StreamStatus.DISABLED.toString());
        actual = adapter.getStreamStatus();
        assertEquals(com.amazonaws.services.kinesis.model.StreamStatus.ACTIVE.toString(), actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedStreamStatus() {
        when(mockDescription.getStreamStatus()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamStatus();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamStatusFailure() {
        adapter.setStreamStatus(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetStreamStatusAsType() {
        adapter.setStreamStatus(com.amazonaws.services.kinesis.model.StreamStatus.CREATING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamStatusFailure() {
        adapter.withStreamStatus(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithStreamStatusAsType() {
        adapter.withStreamStatus(com.amazonaws.services.kinesis.model.StreamStatus.ACTIVE);
    }

    @Test
    public void testGetShardsWithItem() {
        java.util.List<com.amazonaws.services.kinesis.model.Shard> shardList = adapter.getShards();
        assertEquals(1, shardList.size());
        assertTrue(shardList.get(0) instanceof ShardAdapter);
    }

    @Test
    public void testGetShardsWithNoItems() {
        when(mockDescription.getShards()).thenReturn(new java.util.ArrayList<Shard>());
        StreamDescriptionAdapter localAdapter = new StreamDescriptionAdapter(mockDescription);
        java.util.List<com.amazonaws.services.kinesis.model.Shard> shardList = localAdapter.getShards();
        assertTrue(shardList.isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShards() {
        adapter.setShards(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithShards() {
        adapter.withShards(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithShards2() {
        final Collection<com.amazonaws.services.kinesis.model.Shard> shards = Collections.emptyList();
        adapter.withShards(shards);
    }

    @Test
    public void testIsHasMoreShardsTrue() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(TEST_STRING);
        assertTrue(adapter.isHasMoreShards());
    }

    @Test
    public void testIsHasMoreShardsFalse() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(null);
        assertFalse(adapter.isHasMoreShards());
    }

    @Test
    public void testGetHasMoreShardsTrue() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(TEST_STRING);
        assertTrue(adapter.getHasMoreShards());
    }

    @Test
    public void testGetHasMoreShardsFalse() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(null);
        assertFalse(adapter.getHasMoreShards());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetHasMoreShards() {
        adapter.setHasMoreShards(false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithHasMoreShards() {
        adapter.withHasMoreShards(true);
    }

    @Test
    public void testRealDataNoShards() {
        StreamDescription stream = createStreamDescription(false);
        StreamDescriptionAdapter streamAdapter = new StreamDescriptionAdapter(stream);
        assertEquals(stream.getStreamArn(), streamAdapter.getStreamName());
        assertEquals(stream.getStreamArn(), streamAdapter.getStreamARN());
        assertEquals(stream.getShards().size(), streamAdapter.getShards().size());
    }

    @Test
    public void testRealDataWithShards() {
        StreamDescription stream = createStreamDescription(true);
        StreamDescriptionAdapter streamAdapter = new StreamDescriptionAdapter(stream);
        assertEquals(stream.getStreamArn(), streamAdapter.getStreamName());
        assertEquals(stream.getStreamArn(), streamAdapter.getStreamARN());
        assertEquals(stream.getShards().size(), streamAdapter.getShards().size());
    }

    @Test
    public void testGetInternalObject() {
        com.amazonaws.services.kinesis.model.StreamDescription kinesisStreamDescription = new StreamDescriptionAdapter(mockDescription);
        StreamDescription internalObject = ((StreamDescriptionAdapter) kinesisStreamDescription).getInternalObject();
        assertSame(mockDescription, internalObject);
    }

    private StreamDescription createStreamDescription(Boolean withShards) {
        java.util.List<Shard> shards = new java.util.ArrayList<Shard>();
        if (withShards) {
            shards.add(new Shard());
        }
        return new StreamDescription().withStreamArn(TEST_STRING).withShards(shards);
    }

}
