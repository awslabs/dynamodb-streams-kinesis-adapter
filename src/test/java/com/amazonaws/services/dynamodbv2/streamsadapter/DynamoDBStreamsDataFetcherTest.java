/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsGetRecordsResponseAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.NullMetricsFactory;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import software.amazon.kinesis.retrieval.KinesisDataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName;
import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class DynamoDBStreamsDataFetcherTest {

    private static final String STREAM_NAME = createKinesisStreamIdentifierFromDynamoDBStreamsArn("arn:aws:dynamodb:us-west-2:111122223333:table/Forum/stream/2015-05-20T20:51:10.252", false);
    private static final String SHARD_ID = "shard-000001";
    private static final String SEQUENCE_NUMBER = "123";
    private static final String ITERATOR = "iterator-123";
    private static final int MAX_RECORDS = 100;

    private AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient;

    private DynamoDBStreamsDataFetcher dynamoDBStreamsDataFetcher;
    private StreamIdentifier streamIdentifier;
    private DataFetcherProviderConfig dataFetcherProviderConfig;
    private MetricsFactory metricsFactory;

    @BeforeEach
    void setup() {
        amazonDynamoDBStreamsAdapterClient = Mockito.mock(AmazonDynamoDBStreamsAdapterClient.class);
        streamIdentifier = StreamIdentifier.singleStreamInstance(STREAM_NAME);
        metricsFactory = new NullMetricsFactory();
        dataFetcherProviderConfig = new KinesisDataFetcherProviderConfig(streamIdentifier, SHARD_ID, metricsFactory, MAX_RECORDS, Duration.ofMillis(30000L));
        dynamoDBStreamsDataFetcher = new DynamoDBStreamsDataFetcher(amazonDynamoDBStreamsAdapterClient, dataFetcherProviderConfig);
    }

    @Test
    void testInitializeWithSequenceNumber() {
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);

        dynamoDBStreamsDataFetcher.initialize(
                SEQUENCE_NUMBER,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        );
        assertTrue(dynamoDBStreamsDataFetcher.isInitialized());
        assertEquals(SEQUENCE_NUMBER, dynamoDBStreamsDataFetcher.getLastKnownSequenceNumber());
        assertEquals(ITERATOR, dynamoDBStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testInitializeWithTrimHorizon() {
        mockGetShardIterator(null, ShardIteratorType.TRIM_HORIZON, ITERATOR);

        dynamoDBStreamsDataFetcher.initialize(
                ExtendedSequenceNumber.TRIM_HORIZON,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
        );

        assertTrue(dynamoDBStreamsDataFetcher.isInitialized());
        assertEquals(dynamoDBStreamsDataFetcher.getLastKnownSequenceNumber(), InitialPositionInStream.TRIM_HORIZON.toString());
        assertEquals(ITERATOR, dynamoDBStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testInitializeWithLatest() {
        mockGetShardIterator(null, ShardIteratorType.LATEST, ITERATOR);

        dynamoDBStreamsDataFetcher.initialize(
                ExtendedSequenceNumber.LATEST,
                InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
        );

        assertTrue(dynamoDBStreamsDataFetcher.isInitialized());
        assertEquals(dynamoDBStreamsDataFetcher.getLastKnownSequenceNumber(), InitialPositionInStream.LATEST.toString());
        assertEquals(ITERATOR, dynamoDBStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testDdbGetRecordsWithoutInitialization() {
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> dynamoDBStreamsDataFetcher.getRecords()
        );
        assertEquals("DynamoDBStreamsDataFetcher.getRecords method called before initialization.", exception.getMessage());
    }

    @Test
    void testDdbGetRecordsSuccess() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        dynamoDBStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        Record record1 = createRecord("1");
        Record record2 = createRecord("2");
        GetRecordsResponseAdapter response = new DynamoDBStreamsGetRecordsResponseAdapter(
                GetRecordsResponse.builder()
                        .records(Arrays.asList(record1, record2))
                        .nextShardIterator("next-iterator")
                        .build());

        when(amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> response));

        // Execute
        DataFetcherResult result = dynamoDBStreamsDataFetcher.getRecords();

        // Verify
        assertNotNull(result);
        assertEquals(2, result.getResultAdapter().records().size());
        assertEquals("next-iterator", result.getResultAdapter().nextShardIterator());
        assertFalse(result.isShardEnd());
    }

    @Test
    void testDdbGetRecordsWithResourceNotFound() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        dynamoDBStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        when(amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(any(GetRecordsRequest.class)))
                .thenThrow(ResourceNotFoundException.builder().build());

        // Execute
        DataFetcherResult result = dynamoDBStreamsDataFetcher.getRecords();

        // Verify
        assertNotNull(result);
        assertTrue(result.getResultAdapter().records().isEmpty());
        assertNull(result.getResultAdapter().nextShardIterator());
    }

    @Test
    void testShardEndReached() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        dynamoDBStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        GetRecordsResponse response = GetRecordsResponse.builder()
                .records(Collections.emptyList())
                .nextShardIterator(null)  // Null iterator indicates shard end
                .build();

        when(amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new DynamoDBStreamsGetRecordsResponseAdapter(response)));

        // Execute
        DataFetcherResult result = dynamoDBStreamsDataFetcher.getRecords();
        result.acceptAdapter();

        // Verify
        assertTrue(dynamoDBStreamsDataFetcher.isShardEndReached());
    }

    @Test
    void testRestartIterator() {
        // Setup
        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, ITERATOR);
        dynamoDBStreamsDataFetcher.initialize(SEQUENCE_NUMBER, InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST));

        mockGetShardIterator(SEQUENCE_NUMBER, ShardIteratorType.AFTER_SEQUENCE_NUMBER, "new-iterator");

        // Execute
        dynamoDBStreamsDataFetcher.restartIterator();

        // Verify
        assertEquals("new-iterator", dynamoDBStreamsDataFetcher.getNextIterator());
    }

    @Test
    void testRestartIteratorWithoutInitialization() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> dynamoDBStreamsDataFetcher.restartIterator()
        );
        assertEquals("Make sure to initialize the DynamoDBStreamsDataFetcher before restarting the iterator.",
                exception.getMessage());
    }

    private void mockGetShardIterator(String sequenceNumber, ShardIteratorType iteratorType, String iterator) {
        when(amazonDynamoDBStreamsAdapterClient.getShardIterator(
                GetShardIteratorRequest.builder()
                        .streamName(createDynamoDBStreamsArnFromKinesisStreamName(STREAM_NAME))
                        .shardId(SHARD_ID)
                        .startingSequenceNumber(sequenceNumber)
                        .shardIteratorType(iteratorType)
                        .build())
        )
                .thenReturn(
                        CompletableFuture.supplyAsync(() ->
                                GetShardIteratorResponse.builder()
                                        .shardIterator(iterator)
                                        .build()
                        )
                );
    }

    private Record createRecord(String sequenceNumber) {
        return Record.builder()
                .dynamodb(software.amazon.awssdk.services.dynamodb.model.StreamRecord.builder()
                        .sequenceNumber(sequenceNumber)
                        .approximateCreationDateTime(Instant.now())
                        .build())
                .build();
    }


    @Test
    void testGetDdbGetRecordsResponseWithShardEndAndDisabledStream() throws Exception {
        // Setup
        GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator("some-iterator")
                .limit(100)
                .build();

        // Create GetRecordsResponse with null nextShardIterator
        GetRecordsResponse recordsResponse = GetRecordsResponse.builder()
                .records(Collections.emptyList())
                .nextShardIterator(null)
                .build();

        // Create DescribeStream response for disabled stream
        DescribeStreamResponse describeStreamResponse = DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .streamName(STREAM_NAME)
                        .streamStatus(StreamStatus.DISABLED.toString())
                        .shards(Collections.emptyList())
                        .hasMoreShards(false)
                        .build())
                .build();

        // Mock responses
        when(amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new DynamoDBStreamsGetRecordsResponseAdapter(recordsResponse)));

        // Execute
        GetRecordsResponseAdapter result = dynamoDBStreamsDataFetcher.getGetRecordsResponse(request);

        // Verify
        assertNotNull(result);
        assertTrue(result.records().isEmpty());
        assertNull(result.nextShardIterator());
        assertTrue(result.childShards().isEmpty());
    }


    @Test
    void testGetDdbGetRecordsResponseWithActiveIterator() throws Exception {
        // Setup
        GetRecordsRequest request = GetRecordsRequest.builder()
                .shardIterator("active-iterator")
                .limit(100)
                .build();

        // Create GetRecordsResponse with active iterator
        GetRecordsResponse recordsResponse = GetRecordsResponse.builder()
                .records(Collections.emptyList())
                .nextShardIterator("next-iterator")  // Active iterator
                .build();

        when(amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(any(GetRecordsRequest.class)))
                .thenReturn(CompletableFuture.supplyAsync(() -> new DynamoDBStreamsGetRecordsResponseAdapter(recordsResponse)));

        // Execute
        GetRecordsResponseAdapter result = dynamoDBStreamsDataFetcher.getGetRecordsResponse(request);

        // Verify
        assertNotNull(result);
        assertTrue(result.records().isEmpty());
        assertEquals("next-iterator", result.nextShardIterator());
    }
}
