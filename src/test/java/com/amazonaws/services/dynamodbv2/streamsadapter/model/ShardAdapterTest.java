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

import com.amazonaws.services.dynamodbv2.model.SequenceNumberRange;
import com.amazonaws.services.dynamodbv2.model.Shard;

public class ShardAdapterTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private Shard mockShard;

    @Mock
    private SequenceNumberRange mockSequenceNumberRange;

    private ShardAdapter adapter;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapter = new ShardAdapter(mockShard);
        when(mockShard.getSequenceNumberRange()).thenReturn(mockSequenceNumberRange);
    }

    @Test
    public void testGetShardId() {
        when(mockShard.getShardId()).thenReturn(TEST_STRING);
        String actual = adapter.getShardId();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetShardId() {
        adapter.setShardId(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithShardId() {
        adapter.withShardId(TEST_STRING);
    }

    @Test
    public void testGetParentShardId() {
        when(mockShard.getParentShardId()).thenReturn(TEST_STRING);
        String actual = adapter.getParentShardId();
        assertEquals(TEST_STRING, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetParentShardId() {
        adapter.setParentShardId(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithParentShardId() {
        adapter.withParentShardId(TEST_STRING);
    }

    @Test
    public void testGetAdjacentParentShardId() {
        String actual = adapter.getAdjacentParentShardId();
        assertEquals(null, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetAdjacentParentShardId() {
        adapter.setAdjacentParentShardId(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithAdjacentParentShardId() {
        adapter.withAdjacentParentShardId(TEST_STRING);
    }

    @Test
    public void testGetHashKeyRange() {
        com.amazonaws.services.kinesis.model.HashKeyRange hashKeyRange = adapter.getHashKeyRange();
        assertEquals(java.math.BigInteger.ZERO.toString(), hashKeyRange.getStartingHashKey());
        assertEquals(java.math.BigInteger.ONE.toString(), hashKeyRange.getEndingHashKey());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetHashKeyRange() {
        adapter.setHashKeyRange(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithHashKeyRange() {
        adapter.withHashKeyRange(null);
    }

    @Test
    public void testGetSequenceNumberRange() {
        when(mockSequenceNumberRange.getStartingSequenceNumber()).thenReturn(TEST_STRING);
        when(mockSequenceNumberRange.getEndingSequenceNumber()).thenReturn(TEST_STRING);
        com.amazonaws.services.kinesis.model.SequenceNumberRange sequenceNumberRange = adapter.getSequenceNumberRange();
        assertEquals(TEST_STRING, sequenceNumberRange.getStartingSequenceNumber());
        assertEquals(TEST_STRING, sequenceNumberRange.getEndingSequenceNumber());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetSequenceNumberRange() {
        adapter.setSequenceNumberRange(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWithSequenceNumberRange() {
        adapter.withSequenceNumberRange(null);
    }

    @Test
    public void testRealData() {
        Shard shard = createShard();
        ShardAdapter shardAdapter = new ShardAdapter(shard);
        assertEquals(shard.getShardId(), shardAdapter.getShardId());
        assertEquals(shard.getParentShardId(), shardAdapter.getParentShardId());
        assertEquals(shard.getSequenceNumberRange().getStartingSequenceNumber(), shardAdapter.getSequenceNumberRange().getStartingSequenceNumber());
        assertEquals(shard.getSequenceNumberRange().getEndingSequenceNumber(), shardAdapter.getSequenceNumberRange().getEndingSequenceNumber());
    }

    private Shard createShard() {
        return new Shard().withShardId(TEST_STRING).withParentShardId(TEST_STRING)
            .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber(TEST_STRING).withEndingSequenceNumber(TEST_STRING));
    }

}
