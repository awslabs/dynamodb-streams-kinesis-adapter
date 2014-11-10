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
package com.amazonaws.services.dynamodbv2.streamsadapter;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Matchers.any;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;

public class AmazonDynamoDBStreamsAdapterClientTest {
    private final String TEST_STRING = "TestString";

    @Mock
    private AmazonDynamoDBStreamsClient mockClient;

    private AmazonDynamoDBStreamsAdapterClient adapterClient;

    @Before
    public void setUpTest() {
        MockitoAnnotations.initMocks(this);
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(mockClient);
        when(mockClient.describeStream(
                any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))
            ).thenReturn(
                new com.amazonaws.services.dynamodbv2.model.DescribeStreamResult()
                    .withStreamDescription(new com.amazonaws.services.dynamodbv2.model.StreamDescription()
                        .withStreamStatus("ENABLED")
                        .withShards(new java.util.ArrayList<com.amazonaws.services.dynamodbv2.model.Shard>()))
            );
        when(mockClient.getShardIterator(
                any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class))
            ).thenReturn(
                new com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult()
            );
        when(mockClient.getRecords(
                any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))
            ).thenReturn(
                new com.amazonaws.services.dynamodbv2.model.GetRecordsResult()
                    .withRecords(new java.util.ArrayList<com.amazonaws.services.dynamodbv2.model.Record>())
            );
        when(mockClient.listStreams(
                any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class))
            ).thenReturn(
                new com.amazonaws.services.dynamodbv2.model.ListStreamsResult()
            );
    }

    @Test
    public void testSetEndpoint() {
        adapterClient.setEndpoint(TEST_STRING);
        verify(mockClient).setEndpoint(TEST_STRING);
    }

    @Test
    public void testSetRegion() {
        adapterClient.setRegion(Region.getRegion(Regions.US_WEST_2));
        verify(mockClient).setRegion(Region.getRegion(Regions.US_WEST_2));
    }

    @Test
    public void testDescribeStream() {
        DescribeStreamRequest request = new DescribeStreamRequest();
        Object result = adapterClient.describeStream(request);
        assertTrue(result instanceof DescribeStreamResult);
    }

    @Test
    public void testGetShardIterator() {
        GetShardIteratorRequest request = new GetShardIteratorRequest();
        Object result = adapterClient.getShardIterator(request);
        assertTrue(result instanceof GetShardIteratorResult);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testPutRecord() {
        adapterClient.putRecord(null);
    }

    @Test
    public void testGetRecords() {
        GetRecordsRequest request = new GetRecordsRequest();
        Object result = adapterClient.getRecords(request);
        assertTrue(result instanceof GetRecordsResult);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testSplitShard() {
        adapterClient.splitShard(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testCreateStream() {
        adapterClient.createStream(null);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testDeleteStream() {
        adapterClient.deleteStream(TEST_STRING);
    }

    @Test
    public void testListStreams() {
        ListStreamsRequest request = new ListStreamsRequest();
        Object result = adapterClient.listStreams(request);
        assertTrue(result instanceof ListStreamsResult);
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testMergeShards() {
        adapterClient.mergeShards(null);
    }

    @Test
    public void testShutdown() {
        adapterClient.shutdown();
        verify(mockClient).shutdown();
    }

    @Test(expected=UnsupportedOperationException.class)
    public void testGetCachedResponseMetadata() {
        adapterClient.getCachedResponseMetadata(null);
    }

}
