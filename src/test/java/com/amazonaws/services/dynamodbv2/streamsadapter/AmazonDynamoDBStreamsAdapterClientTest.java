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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AmazonDynamoDBStreamsAdapterClientTest {

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;
    @Mock
    private DynamoDbStreamsClient dynamoDbStreamsClient;

    private Region region = Region.US_WEST_2;

    private AmazonDynamoDBStreamsAdapterClient adapterClient;

    private static final String STREAM_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
    private static final String SHARD_ID = "shardId-00000000000000000000-00000000";
    private static final String SEQUENCE_NUMBER = "100";
    private static final String ITERATOR = "iterator-value";

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDbStreamsClient);
    }

    @Test
    void testDescribeStream() {
        // Setup DynamoDB response
        DescribeStreamResponse dynamoResponse = DescribeStreamResponse.builder()
                .streamDescription(StreamDescription.builder()
                        .streamArn(STREAM_ARN)
                        .streamStatus(StreamStatus.ENABLED)
                        .shards(Collections.emptyList())
                        .build())
                .build();

        when(dynamoDbStreamsClient.describeStream(any(DescribeStreamRequest.class)))
                .thenReturn(dynamoResponse);

        // Execute
        software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest.builder()
                        .streamName(STREAM_ARN)
                        .limit(100)
                        .exclusiveStartShardId(SHARD_ID)
                        .build();

        software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse response =
                adapterClient.describeStream(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(STREAM_ARN, response.streamDescription().streamName());
        assertEquals("ENABLED", response.streamDescription().streamStatusAsString());
    }

    @Test
    void testGetShardIterator() {
        // Setup DynamoDB response
        GetShardIteratorResponse dynamoResponse = GetShardIteratorResponse.builder()
                .shardIterator(ITERATOR)
                .build();

        when(dynamoDbStreamsClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenReturn(dynamoResponse);

        // Execute
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder()
                        .streamName(STREAM_ARN)
                        .shardId(SHARD_ID)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber(SEQUENCE_NUMBER)
                        .build();

        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse response =
                adapterClient.getShardIterator(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(ITERATOR, response.shardIterator());
    }

    @Test
    void testGetShardIteratorWithTrimmedDataAndSkip() {
        // Setup initial failure with TrimmedDataAccessException
        when(dynamoDbStreamsClient.getShardIterator(any(GetShardIteratorRequest.class)))
                .thenThrow(TrimmedDataAccessException.builder().build())
                .thenReturn(GetShardIteratorResponse.builder().shardIterator(ITERATOR).build());

        adapterClient.setSkipRecordsBehavior(
                AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        // Execute
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest.builder()
                        .streamName(STREAM_ARN)
                        .shardId(SHARD_ID)
                        .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                        .startingSequenceNumber(SEQUENCE_NUMBER)
                        .build();

        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse response =
                adapterClient.getShardIterator(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(ITERATOR, response.shardIterator());
    }

    @Test
    void testListStreams() {
        // Setup DynamoDB response
        ListStreamsResponse dynamoResponse = ListStreamsResponse.builder()
                .streams(Arrays.asList(
                        Stream.builder().streamArn(STREAM_ARN).tableName("TestTable").build()))
                .lastEvaluatedStreamArn(STREAM_ARN)
                .build();

        when(dynamoDbStreamsClient.listStreams(any(ListStreamsRequest.class)))
                .thenReturn(dynamoResponse);

        // Execute
        software.amazon.awssdk.services.kinesis.model.ListStreamsRequest kinesisRequest =
                software.amazon.awssdk.services.kinesis.model.ListStreamsRequest.builder()
                        .limit(100)
                        .exclusiveStartStreamName(STREAM_ARN)
                        .build();

        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse response =
                adapterClient.listStreams(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(1, response.streamNames().size());
        assertEquals(STREAM_ARN, response.streamNames().get(0));
        assertTrue(response.hasMoreStreams());
    }

    @Test
    void testGetRecords() {
        // Setup DynamoDB response
        GetRecordsResponse dynamoResponse = GetRecordsResponse.builder()
                .records(Collections.singletonList(
                        software.amazon.awssdk.services.dynamodb.model.Record.builder()
                                .dynamodb(StreamRecord.builder()
                                        .sequenceNumber(SEQUENCE_NUMBER)
                                        .approximateCreationDateTime(Instant.now())
                                        .build())
                                .build()))
                .nextShardIterator("next-" + ITERATOR)
                .build();

        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(dynamoResponse);

        // Execute
        GetRecordsRequest kinesisRequest =
                GetRecordsRequest.builder()
                        .shardIterator(ITERATOR)
                        .limit(100)
                        .build();

        GetRecordsResponseAdapter response =
                adapterClient.getDynamoDBStreamsRecords(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertEquals(1, response.records().size());
        assertEquals("next-" + ITERATOR, response.nextShardIterator());
        assertNotNull(response.millisBehindLatest());
    }

    @Test
    void testGetRecordsWithEmptyRecords() {
        // Setup DynamoDB response with empty records
        GetRecordsResponse dynamoResponse = GetRecordsResponse.builder()
                .records(Collections.emptyList())
                .nextShardIterator("next-" + ITERATOR)
                .build();

        when(dynamoDbStreamsClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(dynamoResponse);

        // Execute
        GetRecordsRequest kinesisRequest = GetRecordsRequest.builder()
                .shardIterator(ITERATOR)
                .limit(100)
                .build();

        GetRecordsResponseAdapter response =
                adapterClient.getDynamoDBStreamsRecords(kinesisRequest).join();

        // Verify
        assertNotNull(response);
        assertTrue(response.records().isEmpty());
        assertEquals("next-" + ITERATOR, response.nextShardIterator());
        assertNull(response.millisBehindLatest());
    }

    @Test
    void testSetSkipRecordsBehavior() {
        // Test setting valid behavior
        adapterClient.setSkipRecordsBehavior(
                AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
        assertEquals(
                AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON,
                adapterClient.getSkipRecordsBehavior());

        adapterClient.setSkipRecordsBehavior(
                AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY);
        assertEquals(
                AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY,
                adapterClient.getSkipRecordsBehavior());

        // Test setting null behavior
        assertThrows(NullPointerException.class, () -> adapterClient.setSkipRecordsBehavior(null));
    }

    @Test
    void testServiceName() {
        when(dynamoDbStreamsClient.serviceName()).thenReturn("dynamodb-streams");
        assertEquals("dynamodb-streams", adapterClient.serviceName());
    }

    @Test
    void testClose() {
        adapterClient.close();
        verify(dynamoDbStreamsClient).close();
    }
}