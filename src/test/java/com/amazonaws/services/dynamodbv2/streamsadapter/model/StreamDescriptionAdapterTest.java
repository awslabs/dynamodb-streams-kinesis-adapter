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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.ShardAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.StreamDescriptionAdapter;

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
        when(mockDescription.getStreamId()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamName();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamName() {
        adapter.setStreamName(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithStreamName() {
        adapter.withStreamName(TEST_STRING);
    }

    @Test
    public void testGetStreamARN() {
        when(mockDescription.getStreamARN()).thenReturn(TEST_STRING);
        String actual = adapter.getStreamARN();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamARN() {
        adapter.setStreamARN(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
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

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamStatusFailure() {
        adapter.setStreamStatus(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetStreamStatusAsType() {
        adapter.setStreamStatus(com.amazonaws.services.kinesis.model.StreamStatus.CREATING);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithStreamStatusFailure() {
        adapter.withStreamStatus(TEST_STRING);
    }

    @Test(expected=UnsupportedOperationException.class)
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
        assertEquals(0, shardList.size());
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetShards() {
        adapter.setShards(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithShards() {
        adapter.withShards(null, null);
    }

    @Test
    public void testIsHasMoreShards() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(null);
        Boolean result = adapter.isHasMoreShards();
        assertFalse(result);
    }

    @Test
    public void testGetHasMoreShards() {
        when(mockDescription.getLastEvaluatedShardId()).thenReturn(TEST_STRING);
        Boolean result = adapter.getHasMoreShards();
        assertTrue(result);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSetHasMoreShards() {
        adapter.setHasMoreShards(false);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testWithHasMoreShards() {
        adapter.withHasMoreShards(true);
    }

    @Test
    public void testRealDataNoShards() {
        StreamDescription stream = createStreamDescription(false);
        StreamDescriptionAdapter streamAdapter = new StreamDescriptionAdapter(stream);
        assertEquals(stream.getStreamId(), streamAdapter.getStreamName());
        assertEquals(stream.getStreamARN(), streamAdapter.getStreamARN());
        assertEquals(stream.getShards().size(), streamAdapter.getShards().size());
    }

    @Test
    public void testRealDataWithShards() {
        StreamDescription stream = createStreamDescription(true);
        StreamDescriptionAdapter streamAdapter = new StreamDescriptionAdapter(stream);
        assertEquals(stream.getStreamId(), streamAdapter.getStreamName());
        assertEquals(stream.getStreamARN(), streamAdapter.getStreamARN());
        assertEquals(stream.getShards().size(), streamAdapter.getShards().size());
    }

    private StreamDescription createStreamDescription(Boolean withShards) {
        java.util.List<Shard> shards = new java.util.ArrayList<Shard>();
        if(withShards) {
            shards.add(new Shard());
        }
        return new StreamDescription()
            .withStreamId(TEST_STRING)
            .withStreamARN(TEST_STRING)
            .withShards(shards);
    }

}
