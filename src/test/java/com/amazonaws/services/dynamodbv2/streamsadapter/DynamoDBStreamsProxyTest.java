/*
 *  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.streamsadapter.utils.ThreadSleeper;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.model.StreamStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamoDBStreamsProxyTest {

    private static final String STREAM_NAME = "StreamName";
    private static final String STARTING_SEQUENCE_NUMBER = "1";
    private static final String ENDING_SEQUENCE_NUMBER = "2";
    private static final String NULL_SEQUENCE_NUMBER = null;
    private static final boolean INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED = true;
    private static final long INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS = 200L; // Multiplier for exponential back-off
    private static final long INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS = 1200L; // Base for exponential back-off
    private static final Double[] RANDOM_SEQUENCE = new Double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.2, 0.4, 0.6, 0.8, 0.9, 0.3};
    private static final Long MAX_SHARD_COUNT_TO_TRIGGER_RETRIES = 1500L;
    private static final int MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES = 5;
    private static final int DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED = 3;
    private static final int NUM_SHARDS = 7;
    private static final int DEFAULT_FIRST_SHARD_PARENT_ID = 0;
    private static final boolean LEAF_NODE_CLOSED = true;
    private static final boolean LEAF_NODE_OPEN = false;
    private static final boolean HAS_MORE_SHARDS = true;
    private static final boolean NO_MORE_SHARDS = false;

    @Mock
    private AWSCredentialsProvider mockAwsCredentialsProvider;

    @Mock
    private AmazonKinesis mockKinesisClient;

    @Mock
    private ThreadSleeper mockSleeper;

    @Mock
    private Random mockRandom;

    private DynamoDBStreamsProxy dynamoDBStreamsProxy;

    @Before
    public void setup() throws InterruptedException {
        MockitoAnnotations.initMocks(this);
        Mockito.doNothing().when(mockSleeper).sleep(anyLong());
        dynamoDBStreamsProxy = new DynamoDBStreamsProxy
            .Builder(STREAM_NAME, mockAwsCredentialsProvider, mockKinesisClient)
            .withSleeper(mockSleeper)
            .withMaxRetriesToResolveInconsistencies(MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES)
            .withMaxDescribeStreamRetryAttempts(DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED)
            .withInconsistencyResolutionRetryBackoffBaseInMillis(INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS)
            .withInconsistencyResolutionRetryBackoffMultiplierInMillis(INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS)
            .withInconsistencyResolutionRetryBackoffJitterEnabled(INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_JITTER_ENABLED)
            .withRandomNumberGeneratorForJitter(mockRandom)
            .build();
        when(mockRandom.nextDouble()).thenAnswer(new Answer() {
            private int count = 0;
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                return RANDOM_SEQUENCE[count++];
            }
        });
    }

    @Test
    public void testDDBProxyRetriesOnceToResolveNoOpenChildrenInShardGraph() {
        final int numberOfInconsistentResults = 1;
        final boolean endWithConsistentGraph = true;
        executeGetShardListTest(numberOfInconsistentResults, endWithConsistentGraph);
    }

    @Test
    public void testDDBProxyRetriesUpToSpecifiedMaxTimesToResolveNoOpenChildrenInShardGraph() {
        final boolean endWithConsistentGraph = true;
        executeGetShardListTest(MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES, endWithConsistentGraph);
    }

    @Test
    public void testDDBProxyDoesNotRetryMoreThanSpecifiedMaxTimesToResolveNoOpenChildrenInShardGraph() {
        final boolean endWithConsistentGraph = false;
        executeGetShardListTest(MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES, endWithConsistentGraph);
    }

    @Test
    public void testDDBProxyDoesNotRetryIfShardGraphHasAllChildrenOpen() {
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, NO_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        verify(mockKinesisClient, times(1)).describeStream(argumentCaptor.capture());
        Assert.assertEquals(null, argumentCaptor.getValue().getExclusiveStartShardId());
        Assert.assertEquals(NUM_SHARDS, result.size());
        verifyExpectedShardsInResult(shards, result);
    }

    @Test(expected = LimitExceededException.class)
    public void testDDBProxyThrowsWhenDescribeStreamIsThrottledDuringGetShardList() {
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, HAS_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult)
            .thenThrow(new LimitExceededException("Test"));
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        try {
            dynamoDBStreamsProxy.getShardList();
            fail("Should have thrown a LimitExceededException and not reached here.");
        } catch (final LimitExceededException le) {
            int expectedNumberOfInvocations = DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED + 1;
            verify(mockKinesisClient, times(expectedNumberOfInvocations)).describeStream(argumentCaptor.capture());
            throw le;
        }
    }

    @Test(expected = ResourceNotFoundException.class)
    public void testDDBProxyThrowsRNFEWhenDescribeStreamIsThrottledDuringGetShardList() {
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, HAS_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult)
            .thenThrow(new ResourceNotFoundException("Test"));
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        try {
            dynamoDBStreamsProxy.getShardList();
            fail("Should have thrown a ResourceNotFoundException and not reached here.");
        } catch (final ResourceNotFoundException rnfe) {
            verify(mockKinesisClient, times(2)).describeStream(argumentCaptor.capture());
            throw rnfe;
        }
    }

    @Test
    public void testDDBProxyReturnsNullWhenStreamIsDisabledDuringGetShardList() {
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, NO_MORE_SHARDS);
        describeStreamResult.setStreamDescription(new StreamDescription().withStreamStatus("DISABLED"));
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        List<Shard> result = dynamoDBStreamsProxy.getShardList();
        verify(mockKinesisClient, times(1)).describeStream(argumentCaptor.capture());
        Assert.assertNull("Response from getShardList should be null. ", result);
    }

    @Test
    public void testDDBProxyReturnsNullWhenStreamIsDisabledWhenFixingInconsistencies() {
        final String[] expectedExclusiveShardIdSequence = new String[] {
            null,
            Integer.toString(7),
        };
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_CLOSED);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, NO_MORE_SHARDS);
        final DescribeStreamResult disabledDescribeStreamResult = new DescribeStreamResult();
        disabledDescribeStreamResult.setStreamDescription(new StreamDescription().withStreamStatus("DISABLED"));
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult)
            .thenReturn(disabledDescribeStreamResult);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfSleeperInvocations = 1;
        // 2 calls, the second one when trying to resolve inconsistencies
        verify(mockKinesisClient, times(2)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(argumentCaptor.getAllValues(), Arrays.asList(expectedExclusiveShardIdSequence));
        verifyBackoffIntervals(sleeperArgumentCaptor.getAllValues());
        Assert.assertNull("Response from getShardList should be null. ", result);
    }

    /**
     * This test specifically tests that sorting/ordering of closed leaf node shard ids
     * within ShardGraph works as expected for the format of shardId actually used in DynamoDB.
     * This also tests the behavior that when an intermediate page in DescribeStream response fixes the
     * shard graph, no more describe stream calls are made even though there might be more shards that
     * can be fetched. This design makes sure that the attempts to fix the shard graph do not add more
     * shards to the graph, thereby preventing a possible infinite loop if more closed leaf node shards are
     * present in the subsequent pages.
     */
    @Test
    public void testShardGraphResolutionOccursFromEarliestShardWhenMultipleClosedLeafNodeShardsArePresent() {
        // create shard Ids using the format used in DynamoDB
        String shardId1 = "shardId-00000001517312623906-fc3dbd40";
        String shardId2 = "shardId-00000001517312607815-45ecd9d9";
        String shardId3 = "shardId-00000001517312572197-a2ebf9ee";
        // expected to fetch starting from the earliest/first shard id in the set.
        final String[] expectedExclusiveShardIdSequence = new String[] {
            null,
            "shardId-00000001517312572197-a2ebf9ee",
            "shardId-00000001517312607815-45ecd9d9",
            "shardId-00000001517312623906-fc3dbd40",
        };
        final List<Shard> allShards = new LinkedList<>();
        final List<Shard> page1 = getShardList(10, NUM_SHARDS, LEAF_NODE_CLOSED);
        page1.get(page1.size() - 1).setShardId(shardId1);
        final List<Shard> page2 = getShardList(20, NUM_SHARDS, LEAF_NODE_CLOSED);
        page2.get(page2.size() - 1).setShardId(shardId2);
        final List<Shard> page3 = getShardList(30, NUM_SHARDS, LEAF_NODE_CLOSED);
        page3.get(page3.size() - 1).setShardId(shardId3);
        page1.addAll(page2);
        page1.addAll(page3);
        final DescribeStreamResult ds_result = getDescribeStreamResult(page1, NO_MORE_SHARDS);
        final Shard openShard1 = new Shard().withParentShardId(shardId3).withShardId("OpenShard1");
        final Shard openShard2 = new Shard().withParentShardId(shardId2).withShardId("OpenShard2");
        final Shard openShard3 = new Shard().withParentShardId(shardId1).withShardId("OpenShard3");
        final List<Shard> openShard1List = new LinkedList<>();
        openShard1List.add(openShard1);
        final DescribeStreamResult os1_page = getDescribeStreamResult(openShard1List, NO_MORE_SHARDS);
        final List<Shard> openShard2List = new LinkedList<>();
        openShard2List.add(openShard2);
        final DescribeStreamResult os2_page = getDescribeStreamResult(openShard2List, NO_MORE_SHARDS);
        final List<Shard> openShard3List = new LinkedList<>();
        openShard3List.add(openShard3);
        allShards.addAll(page1);
        allShards.addAll(openShard1List);
        allShards.addAll(openShard2List);
        allShards.addAll(openShard3List);
        // Setting HasMoreShards to true in DescribeStream response. However, since the tree is fixed now,
        // and the graph is in consistency resolution mode, no more describe stream calls should be made,
        // capping the total DescribeStream calls made at 4.
        final DescribeStreamResult os3_page = getDescribeStreamResult(openShard3List, HAS_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(ds_result)
            .thenReturn(os1_page)
            .thenReturn(os2_page)
            .thenReturn(os3_page);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfSleeperInvocations = 3;
        verify(mockKinesisClient, times(4)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(argumentCaptor.getAllValues(), Arrays.asList(expectedExclusiveShardIdSequence));
        verifyBackoffIntervals(sleeperArgumentCaptor.getAllValues());
        int expectedNumberOfShards = NUM_SHARDS * 3 + 3;
        Assert.assertEquals(expectedNumberOfShards, result.size());
        verifyExpectedShardsInResult(allShards, result);
    }

    /**
     * Tests the following scenario:
     * 1. mockKinesisClient returns 3 pages of 7 shards each, firstShardParentIds are 10, 20 and 30 respectively.
     * 2. Each page constitutes a separate shard lineage.
     * 3. 3rd page has a closed child node.
     * 4. Next call (4th page) returns empty shard list.
     * 5. Next call (5th page) returns one shard which fixes the inconsistent state for the lineage in 3rd page
     */
    @Test
    public void testArbitraryScenario() {
        final String[] expectedExclusiveShardIdSequence = new String[] {
            null,
            Integer.toString(17),
            Integer.toString(27),
            Integer.toString(37),
            Integer.toString(37),
        };
        final List<Shard> allShards = new LinkedList<>();
        final List<Shard> page1 = getShardList(10, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page1);
        final DescribeStreamResult ds_page1 = getDescribeStreamResult(page1, HAS_MORE_SHARDS);
        final List<Shard> page2 = getShardList(20, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page2);
        final DescribeStreamResult ds_page2 = getDescribeStreamResult(page2, HAS_MORE_SHARDS);
        final List<Shard> page3 = getShardList(30, NUM_SHARDS, LEAF_NODE_CLOSED);
        // All shards from this page expected in returned list
        allShards.addAll(page3);
        final DescribeStreamResult ds_page3 = getDescribeStreamResult(page3, NO_MORE_SHARDS);
        final List<Shard> page4 = Collections.emptyList();
        final DescribeStreamResult ds_page4 = getDescribeStreamResult(page4, NO_MORE_SHARDS);
        final List<Shard> page5 = getShardList(37, 1, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page5);
        final DescribeStreamResult ds_page5 = getDescribeStreamResult(page5, NO_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(ds_page1)
            .thenReturn(ds_page2)
            .thenReturn(ds_page3)
            .thenReturn(ds_page4)
            .thenReturn(ds_page5);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfSleeperInvocations = 2; // Inconsistency occurs on page 3; takes 2 calls to resolve it.
        verify(mockKinesisClient, times(MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(argumentCaptor.getAllValues(), Arrays.asList(expectedExclusiveShardIdSequence));
        verifyBackoffIntervals(sleeperArgumentCaptor.getAllValues());
        int expectedNumberOfShards = NUM_SHARDS * 3 + 1;
        Assert.assertEquals(expectedNumberOfShards, result.size());
        verifyExpectedShardsInResult(allShards, result);
    }

    /**
     * Tests the following scenario:
     * 1. mockKinesisClient returns 3 pages of 7 shards each, firstShardParentIds are 10, 20 and 30 respectively.
     * 2. Each page constitutes a separate shard lineage.
     * 3. 3rd page has a closed child node.
     * 4. Next call (4th page) returns a shard list but child has still not appeared due to a lag.
     * 5. Next call (5th page) returns a bunch of shards, the first one of which resolves the inconsistency.
     */
    @Test
    public void testScenarioWhereCorrectChildShardAppearsAfterLag() {
        final String[] expectedExclusiveShardIdSequence = new String[] {
            null,
            Integer.toString(17),
            Integer.toString(27),
            Integer.toString(37),
            Integer.toString(37),
        };
        final List<Shard> allShards = new LinkedList<>();
        final List<Shard> page1 = getShardList(10, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page1);
        final DescribeStreamResult ds_page1 = getDescribeStreamResult(page1, HAS_MORE_SHARDS);
        final List<Shard> page2 = getShardList(20, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page2);
        final DescribeStreamResult ds_page2 = getDescribeStreamResult(page2, HAS_MORE_SHARDS);
        final List<Shard> page3 = getShardList(30, NUM_SHARDS, LEAF_NODE_CLOSED);
        // All shards from this page expected in returned list
        allShards.addAll(page3);
        final DescribeStreamResult ds_page3 = getDescribeStreamResult(page3, NO_MORE_SHARDS);
        // No shards from page4 should show up in the final response
        final List<Shard> page4 = getShardList(39, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult ds_page4 = getDescribeStreamResult(page4, NO_MORE_SHARDS);
        // The first shard from page5 should show up in the final response
        final List<Shard> page5 = getShardList(37, NUM_SHARDS + 2, LEAF_NODE_OPEN);
        //Mark the first shard as open - the first shard will resolve inconsistency
        page5.get(0).setSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("1"));
        allShards.add(page5.get(0));
        final DescribeStreamResult ds_page5 = getDescribeStreamResult(page5, NO_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(ds_page1)
            .thenReturn(ds_page2)
            .thenReturn(ds_page3)
            .thenReturn(ds_page4)
            .thenReturn(ds_page5);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfSleeperInvocations = 2; // Inconsistency occurs on page 3; takes 2 calls to resolve it.
        verify(mockKinesisClient, times(MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(argumentCaptor.getAllValues(), Arrays.asList(expectedExclusiveShardIdSequence));
        verifyBackoffIntervals(sleeperArgumentCaptor.getAllValues());
        int expectedNumberOfShards = NUM_SHARDS * 3 + 1;
        Assert.assertEquals(expectedNumberOfShards, result.size());
        verifyExpectedShardsInResult(allShards, result);
    }

    /**
     * Tests the following scenario:
     * 1. mockKinesisClient returns 3 pages of 7 shards each, firstShardParentIds are 10, 20 and 30 respectively.
     * 2. Each page constitutes a separate shard lineage.
     * 3. 3rd page has a closed child node.
     * 4. Next call (4th page) returns a shard list but child has still not appeared due to a lag.
     * 5. Next 3 calls are throttled, resulting in the LimitExceededException being thrown.
     * 6. Next call (5th page) returns one shard which fixes the inconsistent state for the lineage in 3rd page
     */
    @Test
    public void testProxyBehaviorWhenDescribeStreamIsThrottledDuringAttemptsToFixTree() {
        final String[] expectedExclusiveShardIdSequence = new String[] {
            null,
            Integer.toString(17),
            Integer.toString(27),
            Integer.toString(37),
            Integer.toString(37),
            Integer.toString(37),
            Integer.toString(37),
            Integer.toString(46),
            Integer.toString(37)
        };
        final List<Shard> allShards = new LinkedList<>();
        final List<Shard> page1 = getShardList(10, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page1);
        final DescribeStreamResult ds_page1 = getDescribeStreamResult(page1, HAS_MORE_SHARDS);
        final List<Shard> page2 = getShardList(20, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page2);
        final DescribeStreamResult ds_page2 = getDescribeStreamResult(page2, HAS_MORE_SHARDS);
        final List<Shard> page3 = getShardList(30, NUM_SHARDS, LEAF_NODE_CLOSED);
        // All shards from this page expected in returned list
        allShards.addAll(page3);
        final DescribeStreamResult ds_page3 = getDescribeStreamResult(page3, NO_MORE_SHARDS);
        // No shards from this page expected in returned list
        final List<Shard> page4 = getShardList(39, NUM_SHARDS, LEAF_NODE_OPEN);
        final DescribeStreamResult ds_page4 = getDescribeStreamResult(page4, NO_MORE_SHARDS);
        final List<Shard> page5 = getShardList(46, NUM_SHARDS, LEAF_NODE_OPEN);
        // All shards from this page expected in returned list
        allShards.addAll(page5);
        final DescribeStreamResult ds_page5 = getDescribeStreamResult(page5, NO_MORE_SHARDS);
        final List<Shard> page6 = getShardList(37, NUM_SHARDS + 2, LEAF_NODE_OPEN);
        // Mark the first shard as open - the first shard will resolve inconsistency
        page6.get(0).setSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("1"));
        // First shard from page6 expected in returned list
        allShards.add(page6.get(0));
        final DescribeStreamResult ds_page6 = getDescribeStreamResult(page6, NO_MORE_SHARDS);
        // Create a response sequence with DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED number of LimitExceededExceptions
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(ds_page1)
            .thenReturn(ds_page2)
            .thenReturn(ds_page3)
            .thenReturn(ds_page4)
            .thenThrow(new LimitExceededException("Retry-1"))
            .thenThrow(new LimitExceededException("Retry-2"))
            .thenThrow(new LimitExceededException("Retry-3"))
            .thenReturn(ds_page5)
            .thenReturn(ds_page6);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        boolean limitExceededException = false;
        try {
            dynamoDBStreamsProxy.getShardList();
        } catch (LimitExceededException le) {
            limitExceededException = true;
        }
        Assert.assertTrue("Limit exceeded exception was thrown", limitExceededException);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        // TODO: List all invocations of sleeper
        final int expectedNumberOfSleeperInvocations = DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED + 3;
        // When getShardList is called again after throttling, it will first attempt to build the snapshot, and then
        // go into the loop for fixing it. So total DescribeStream invocations are MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES
        // + DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED + 1 (for the DynamoDBStreamsProxy#buildShardGraphSnapshot
        // DescribeStream call after throttling).
        final int describeStreamNumberOfInvocations =
            MAX_RETRIES_TO_RESOLVE_INCONSISTENCIES + DESCRIBE_STREAM_RETRY_COUNT_WHEN_THROTTLED + 1;
        verify(mockKinesisClient, times(describeStreamNumberOfInvocations)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(argumentCaptor.getAllValues(), Arrays.asList(expectedExclusiveShardIdSequence));
        int expectedNumberOfShards = NUM_SHARDS * 4 + 1;
        verifyExpectedShardsInResult(allShards, result);
        Assert.assertEquals(expectedNumberOfShards, result.size());
    }

    @Test
    public void testNoRetriesOccurToResolveInconsistenciesIfShardCountExceedsMaxLimit() {
        final long numShardsInLineage = 6;
        final long numLineages = MAX_SHARD_COUNT_TO_TRIGGER_RETRIES/numShardsInLineage;
        final List<Shard> allShards = new LinkedList<>();
        // setting leaf node for all but one lineage open.
        for (int i = 1; i < numLineages; i++) {
            final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID + i*10,
                (int)numShardsInLineage, LEAF_NODE_OPEN);
            allShards.addAll(shards);
        }
        // Add one lineage with closed child
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_CLOSED);
        allShards.addAll(shards);
        // Assert shard count is MAX_SHARD_COUNT_TO_TRIGGER_RETRIES + 1
        final int expectedNumberOfShards = MAX_SHARD_COUNT_TO_TRIGGER_RETRIES.intValue() + 1;
        Assert.assertEquals(expectedNumberOfShards, MAX_SHARD_COUNT_TO_TRIGGER_RETRIES  + 1);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(allShards, NO_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class)))
            .thenReturn(describeStreamResult);
        final ArgumentCaptor<DescribeStreamRequest> argumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfSleeperInvocations = 0; // Sleeper not invoked since no retries.
        final int expectedNumberOfKinesisClientInvocations = 1; // Kinesis client not invoked after the first time
        verify(mockKinesisClient, times(expectedNumberOfKinesisClientInvocations)).describeStream(argumentCaptor.capture());
        verify(mockSleeper, times(expectedNumberOfSleeperInvocations)).sleep(sleeperArgumentCaptor.capture());
        Assert.assertNull("ExclusiveStartShardId is null", argumentCaptor.getValue().getExclusiveStartShardId());
        Assert.assertEquals(expectedNumberOfShards, result.size());
        verifyExpectedShardsInResult(allShards, result);
    }

    private void executeGetShardListTest(final int numberOfInconsistentResults, final boolean endWithConsistentGraph) {
        final List<String> exclusiveStartShardIdSequence = getExclusiveShardIdSequenceForDefaultNumShards(numberOfInconsistentResults);
        final List<Shard> shards = getShardList(DEFAULT_FIRST_SHARD_PARENT_ID, NUM_SHARDS, LEAF_NODE_CLOSED);
        final Shard nextShard = createDummyShard(NUM_SHARDS, NUM_SHARDS+1, LEAF_NODE_OPEN);
        final List<Shard> nextShardList = new LinkedList<>();
        nextShardList.add(nextShard);
        final DescribeStreamResult describeStreamResult = getDescribeStreamResult(shards, NO_MORE_SHARDS);
        final DescribeStreamResult nextDescribeStreamResult = getDescribeStreamResult(nextShardList, NO_MORE_SHARDS);
        when(mockKinesisClient.describeStream(any(DescribeStreamRequest.class))).thenAnswer(new Answer() {
            private int count = 0;
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                if (count++ < numberOfInconsistentResults) {
                    return describeStreamResult;
                }
                if (!endWithConsistentGraph) {
                    return describeStreamResult;
                } else {
                    return nextDescribeStreamResult;
                }
            }
        });
        final ArgumentCaptor<DescribeStreamRequest> describeStreamRequestArgumentCaptor = ArgumentCaptor.forClass(DescribeStreamRequest.class);
        final ArgumentCaptor<Long> sleeperArgumentCaptor = ArgumentCaptor.forClass(Long.class);
        final List<Shard> result = dynamoDBStreamsProxy.getShardList();
        final int expectedNumberOfProxyInvocations = numberOfInconsistentResults + 1;
        verify(mockKinesisClient, times(expectedNumberOfProxyInvocations)).describeStream(describeStreamRequestArgumentCaptor.capture());
        verify(mockSleeper, times(numberOfInconsistentResults)).sleep(sleeperArgumentCaptor.capture());
        verifyExclusiveStartShardIdSequence(describeStreamRequestArgumentCaptor.getAllValues(), exclusiveStartShardIdSequence);
        verifyBackoffIntervals(sleeperArgumentCaptor.getAllValues());
        int expectedNumberOfShards = NUM_SHARDS + (endWithConsistentGraph ? 1 : 0);
        Assert.assertEquals(expectedNumberOfShards, result.size());
        final List<Shard> allShards = new LinkedList<>();
        allShards.addAll(shards);
        if (endWithConsistentGraph) {
            allShards.addAll(nextShardList);
        }
        verifyExpectedShardsInResult(allShards, result);
    }

    private Shard createDummyShard(int parentShardId, int shardId, boolean closed) {
        final Shard shard = new Shard();
        shard.setParentShardId(Integer.toString(parentShardId));
        shard.setShardId(Integer.toString(shardId));
        if (closed) {
            shard.setSequenceNumberRange(getSequenceNumberRange());
        } else {
            shard.setSequenceNumberRange(getEndNullSequenceNumberRange());
        }
        return shard;
    }

    private List<Shard> getShardList(int firstShardParentId, int numShards, boolean leafNodeClosed) {
        final List<Shard> shards = new LinkedList<>();
        for (int i = firstShardParentId; i < firstShardParentId + numShards; i++) {
            if (i == firstShardParentId + numShards - 1) {
                shards.add(createDummyShard(i, i+1, leafNodeClosed));
            } else {
                shards.add(createDummyShard(i, i+1, true));
            }
        }
        return shards;
    }

    private void verifyExclusiveStartShardIdSequence(List<DescribeStreamRequest> requests, List<String> expectedSequence) {
        for (int i = 0; i < requests.size(); i++) {
            final DescribeStreamRequest request = requests.get(i);
            final String expectedExclusiveShardId = expectedSequence.get(i);
            Assert.assertEquals(expectedExclusiveShardId, request.getExclusiveStartShardId());
        }
    }

    private void verifyBackoffIntervals(List<Long> backoffIntervals) {
        for (int i = 0; i < backoffIntervals.size(); i++) {
            final long expectedBackoffInterval = getInconsistencyBackoffTimeInMillis(i);
            Assert.assertEquals(expectedBackoffInterval, backoffIntervals.get(i).longValue());
        }
    }

    private void verifyExpectedShardsInResult(List<Shard> expectedShards, List<Shard> actualShards) {
        final HashSet<String> actualShardIdSet = new HashSet<>();
        for (Shard shard : actualShards) {
            actualShardIdSet.add(shard.getShardId());
        }
        for (Shard shard : expectedShards) {
            Assert.assertTrue(actualShardIdSet.contains(shard.getShardId()));
        }
    }

    private DescribeStreamResult getDescribeStreamResult(List<Shard> shards, boolean hasMoreShards) {
        final DescribeStreamResult result = new DescribeStreamResult();
        final StreamDescription streamDescription = new StreamDescription();
        streamDescription.setShards(shards);
        streamDescription.setStreamStatus(StreamStatus.ACTIVE);
        streamDescription.setHasMoreShards(hasMoreShards);
        result.setStreamDescription(streamDescription);
        return result;
    }

    private SequenceNumberRange getSequenceNumberRange() {
        final SequenceNumberRange range = new SequenceNumberRange();
        range.setStartingSequenceNumber(STARTING_SEQUENCE_NUMBER);
        range.setEndingSequenceNumber(ENDING_SEQUENCE_NUMBER);
        return range;
    }

    private SequenceNumberRange getEndNullSequenceNumberRange() {
        final SequenceNumberRange range = new SequenceNumberRange();
        range.setStartingSequenceNumber(STARTING_SEQUENCE_NUMBER);
        range.setEndingSequenceNumber(NULL_SEQUENCE_NUMBER);
        return range;
    }

    private List<String> getExclusiveShardIdSequenceForDefaultNumShards(int count) {
        List<String> shardIdSequence = new LinkedList<>();
        shardIdSequence.add(null);
        for (int i = 0; i< count; i++) {
            shardIdSequence.add(Integer.toString(NUM_SHARDS));
        }
        return shardIdSequence;
    }

    // This replicates the method implemented in DynamoDBStreamsProxy to allow testing jitter
    private long getInconsistencyBackoffTimeInMillis(int retryAttempt) {
        double baseMultiplier = RANDOM_SEQUENCE[retryAttempt];
        return (long)(baseMultiplier * INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_BASE_MILLIS) +
            (long)Math.pow(2.0, retryAttempt) * INCONSISTENCY_RESOLUTION_RETRY_BACKOFF_MULTIPLIER_MILLIS;
    }
}
