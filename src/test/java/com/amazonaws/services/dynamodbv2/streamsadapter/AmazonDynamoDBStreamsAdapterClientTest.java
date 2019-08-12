/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Matchers.isNotNull;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.OperationType;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.SequenceNumberRange;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.model.StreamStatus;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;

@PrepareForTest({AmazonDynamoDBStreamsAdapterClient.class, AmazonDynamoDBStreamsClient.class})
@RunWith(PowerMockRunner.class)
public class AmazonDynamoDBStreamsAdapterClientTest {
    private static final int LIMIT = 50;

    private static final int KINESIS_GET_RECORDS_LIMIT = 10000;

    private static final String STREAM_0000000 = "0000000";

    private static final String SHARD_0000000 = "shard-0000000";

    private static final String SHARD_ITERATOR = "shard-iterator";

    private static final String SHARD_0000001 = "shard-0000001";

    private static final String STREAM_ID = "streamId";

    private static final String TEST_STRING = "TestString";

    private static final String DUMMY_ACCESS_KEY = "dummyAccessKey";

    private static final String DUMMY_SECRET_KEY = "dummySecretKey";

    private static final ClientConfiguration CLIENT_CONFIGURATION = new ClientConfiguration().withProtocol(Protocol.HTTPS).withGzip(true);

    private static final com.amazonaws.auth.AWSCredentials CREDENTIALS = new BasicAWSCredentials(DUMMY_ACCESS_KEY, DUMMY_SECRET_KEY);

    private static final AWSCredentialsProvider CREDENTIALS_PROVIDER = new AWSCredentialsProvider() {

        @Override
        public void refresh() {
            // NOOP
        }

        @Override
        public AWSCredentials getCredentials() {
            return CREDENTIALS;
        }
    };

    private static final RequestMetricCollector REQUEST_METRIC_COLLECTOR = RequestMetricCollector.NONE;

    private static final Map<String, AttributeValue> KEYS = Collections.emptyMap();

    private static final StreamRecord STREAM_RECORD =
        new StreamRecord().withKeys(KEYS).withSequenceNumber(TEST_STRING).withSizeBytes(0L).withStreamViewType(StreamViewType.KEYS_ONLY);

    private static final Record RECORD =
        new Record().withAwsRegion(TEST_STRING).withDynamodb(STREAM_RECORD).withEventID(TEST_STRING).withEventName(OperationType.INSERT).withEventSource(TEST_STRING)
            .withEventVersion(TEST_STRING);

    private static final Stream STREAM = new Stream().withStreamArn(STREAM_ID).withStreamLabel(TEST_STRING).withTableName(TEST_STRING);

    private static final SequenceNumberRange SEQ_NUMBER_RANGE = new SequenceNumberRange().withStartingSequenceNumber("0").withEndingSequenceNumber("1");

    private static final Shard SHARD = new Shard().withShardId(TEST_STRING).withSequenceNumberRange(SEQ_NUMBER_RANGE);

    private static final StreamDescription STREAM_DESCRIPTION =
        new StreamDescription().withCreationRequestDateTime(new Date()).withShards(SHARD).withStreamArn(TEST_STRING).withStreamStatus(StreamStatus.ENABLED)
            .withStreamViewType(StreamViewType.KEYS_ONLY).withTableName(TEST_STRING);

    private static final DescribeStreamResult DESCRIBE_STREAM_RESULT = new DescribeStreamResult().withStreamDescription(STREAM_DESCRIPTION);

    private static final String SERVICE_NAME = "dynamodb";

    private static final String REGION_ID = "us-east-1";

    private AmazonDynamoDBStreamsClient mockClient;

    private AmazonDynamoDBStreamsAdapterClient adapterClient;

    @Before
    public void setUpTest() {
        mockClient = mock(AmazonDynamoDBStreamsClient.class);
        adapterClient = new AmazonDynamoDBStreamsAdapterClient(mockClient);
        when(mockClient.describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))).thenReturn(DESCRIBE_STREAM_RESULT);
        when(mockClient.getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class)))
            .thenReturn(new com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult());
        when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class)))
            .thenReturn(new com.amazonaws.services.dynamodbv2.model.GetRecordsResult().withRecords(new java.util.ArrayList<com.amazonaws.services.dynamodbv2.model.Record>()));
        when(mockClient.listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class)))
            .thenReturn(new com.amazonaws.services.dynamodbv2.model.ListStreamsResult());
    }

    @Test
    public void testConstructorEmpty() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withNoArguments().thenReturn(mockClient);
        new AmazonDynamoDBStreamsAdapterClient();
        verifyNew(AmazonDynamoDBStreamsClient.class).withNoArguments();
    }

    @Test
    public void testConstructorAWSCredentials() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(AWSCredentials.class).withArguments(isA(AWSCredentials.class))
            .then(new Answer<AmazonDynamoDBStreamsClient>() {

                @Override
                public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                    AWSCredentials credentials = invocation.getArgumentAt(0, AWSCredentials.class);
                    assertEquals(DUMMY_ACCESS_KEY, credentials.getAWSAccessKeyId());
                    assertEquals(DUMMY_SECRET_KEY, credentials.getAWSSecretKey());
                    return mockClient;
                }
            });
        new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(AWSCredentials.class));
    }

    @Test
    public void testConstructorAWSCredentialsProvider() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(AWSCredentialsProvider.class).withArguments(isA(AWSCredentialsProvider.class))
            .then(new Answer<AmazonDynamoDBStreamsClient>() {

                @Override
                public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                    AWSCredentialsProvider credentialsProvider = invocation.getArgumentAt(0, AWSCredentialsProvider.class);
                    AWSCredentials credentials = credentialsProvider.getCredentials();
                    assertEquals(DUMMY_ACCESS_KEY, credentials.getAWSAccessKeyId());
                    assertEquals(DUMMY_SECRET_KEY, credentials.getAWSSecretKey());
                    return mockClient;
                }
            });
        new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS_PROVIDER);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(AWSCredentialsProvider.class));
    }

    @Test
    public void testConstructorClientConfiguration() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(ClientConfiguration.class).withArguments(isA(ClientConfiguration.class))
            .then(new Answer<AmazonDynamoDBStreamsClient>() {

                @Override
                public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                    ClientConfiguration clientConfiguration = invocation.getArgumentAt(0, ClientConfiguration.class);
                    assertEquals(CLIENT_CONFIGURATION, clientConfiguration);
                    return mockClient;
                }
            });
        new AmazonDynamoDBStreamsAdapterClient(CLIENT_CONFIGURATION);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(ClientConfiguration.class));
    }

    @Test
    public void testConstructorAWSCredentialsClientConfiguration() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(AWSCredentials.class, ClientConfiguration.class)
            .withArguments(isA(AWSCredentials.class), isA(ClientConfiguration.class)).then(new Answer<AmazonDynamoDBStreamsClient>() {

            @Override
            public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                AWSCredentials credentials = invocation.getArgumentAt(0, AWSCredentials.class);
                ClientConfiguration clientConfiguration = invocation.getArgumentAt(1, ClientConfiguration.class);
                assertEquals(DUMMY_ACCESS_KEY, credentials.getAWSAccessKeyId());
                assertEquals(DUMMY_SECRET_KEY, credentials.getAWSSecretKey());
                assertEquals(CLIENT_CONFIGURATION, clientConfiguration);
                return mockClient;
            }
        });
        new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS, CLIENT_CONFIGURATION);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(AWSCredentials.class), isA(ClientConfiguration.class));
    }

    @Test
    public void testConstructorAWSCredentialsProviderClientConfiguration() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(AWSCredentialsProvider.class, ClientConfiguration.class)
            .withArguments(isA(AWSCredentialsProvider.class), isA(ClientConfiguration.class)).then(new Answer<AmazonDynamoDBStreamsClient>() {

            @Override
            public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                AWSCredentialsProvider credentialsProvider = invocation.getArgumentAt(0, AWSCredentialsProvider.class);
                AWSCredentials credentials = credentialsProvider.getCredentials();
                ClientConfiguration clientConfiguration = invocation.getArgumentAt(1, ClientConfiguration.class);
                assertEquals(DUMMY_ACCESS_KEY, credentials.getAWSAccessKeyId());
                assertEquals(DUMMY_SECRET_KEY, credentials.getAWSSecretKey());
                assertEquals(CLIENT_CONFIGURATION, clientConfiguration);
                return mockClient;
            }
        });
        new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS_PROVIDER, CLIENT_CONFIGURATION);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(AWSCredentialsProvider.class), isA(ClientConfiguration.class));
    }

    @Test
    public void testConstructorAWSCredentialsProviderClientConfigurationRequestMetricsCollector() throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withParameterTypes(AWSCredentialsProvider.class, ClientConfiguration.class, RequestMetricCollector.class)
            .withArguments(isA(AWSCredentialsProvider.class), isA(ClientConfiguration.class), isA(RequestMetricCollector.class)).then(new Answer<AmazonDynamoDBStreamsClient>() {

            @Override
            public AmazonDynamoDBStreamsClient answer(InvocationOnMock invocation) throws Throwable {
                AWSCredentialsProvider credentialsProvider = invocation.getArgumentAt(0, AWSCredentialsProvider.class);
                AWSCredentials credentials = credentialsProvider.getCredentials();
                ClientConfiguration clientConfiguration = invocation.getArgumentAt(1, ClientConfiguration.class);
                RequestMetricCollector requestMetricCollector = invocation.getArgumentAt(2, RequestMetricCollector.class);
                assertEquals(DUMMY_ACCESS_KEY, credentials.getAWSAccessKeyId());
                assertEquals(DUMMY_SECRET_KEY, credentials.getAWSSecretKey());
                assertEquals(CLIENT_CONFIGURATION, clientConfiguration);
                assertEquals(REQUEST_METRIC_COLLECTOR, requestMetricCollector);
                return mockClient;
            }
        });
        new AmazonDynamoDBStreamsAdapterClient(CREDENTIALS_PROVIDER, CLIENT_CONFIGURATION, REQUEST_METRIC_COLLECTOR);
        verifyNew(AmazonDynamoDBStreamsClient.class).withArguments(isA(AWSCredentialsProvider.class), isA(ClientConfiguration.class), isA(RequestMetricCollector.class));
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
        when(mockClient.describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))).thenReturn(DESCRIBE_STREAM_RESULT);
        com.amazonaws.services.kinesis.model.DescribeStreamResult result = adapterClient.describeStream(new DescribeStreamRequest().withStreamName(STREAM_ID));
        com.amazonaws.services.kinesis.model.StreamDescription actual = result.getStreamDescription();
        StreamDescription expected = DESCRIBE_STREAM_RESULT.getStreamDescription();
        assertEquals(expected.getStreamArn(), actual.getStreamARN());
        assertEquals(expected.getShards().size(), actual.getShards().size());
        verify(mockClient).describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class));
    }

    @Test
    public void testDescribeStream2() {
        when(mockClient.describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))).thenAnswer(new Answer<DescribeStreamResult>() {

            @Override
            public DescribeStreamResult answer(InvocationOnMock invocation) throws Throwable {
                final com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest request =
                    invocation.getArgumentAt(0, com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class);
                assertEquals(STREAM_0000000, request.getStreamArn());
                assertEquals(SHARD_0000000, request.getExclusiveStartShardId());
                return DESCRIBE_STREAM_RESULT;
            }

        });
        com.amazonaws.services.kinesis.model.DescribeStreamResult result = adapterClient.describeStream(STREAM_0000000, SHARD_0000000);
        com.amazonaws.services.kinesis.model.StreamDescription actual = result.getStreamDescription();
        StreamDescription expected = DESCRIBE_STREAM_RESULT.getStreamDescription();
        assertEquals(expected.getStreamArn(), actual.getStreamARN());
        assertEquals(expected.getShards().size(), actual.getShards().size());
        verify(mockClient).describeStream(any(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class));
    }

    @Test
    public void testGetShardIterator() {
        when(mockClient.getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class)))
            .thenReturn(new GetShardIteratorResult().withShardIterator(TEST_STRING));
        adapterClient.getShardIterator(new GetShardIteratorRequest());
        verify(mockClient).getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class));
    }

    @Test
    public void testGetShardIterator2() {
        final String shardIteratorType = ShardIteratorType.TRIM_HORIZON.toString();
        when(mockClient.getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class))).thenAnswer(new Answer<GetShardIteratorResult>() {
            @Override
            public GetShardIteratorResult answer(InvocationOnMock invocation) throws Throwable {
                com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest request =
                    invocation.getArgumentAt(0, com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class);
                assertEquals(STREAM_ID, request.getStreamArn());
                assertEquals(SHARD_0000001, request.getShardId());
                assertEquals(shardIteratorType, request.getShardIteratorType());
                return new GetShardIteratorResult().withShardIterator(SHARD_ITERATOR);
            }

        });
        assertEquals(SHARD_ITERATOR, adapterClient.getShardIterator(STREAM_ID, SHARD_0000001, shardIteratorType).getShardIterator());
        verify(mockClient).getShardIterator(any(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecord() {
        adapterClient.putRecord(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecord2() {
        adapterClient.putRecord(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecord3() {
        adapterClient.putRecord(null, null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPutRecords() {
        adapterClient.putRecords(null);
    }

    @Test
    public void testGetRecords() {
        when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).thenReturn(new GetRecordsResult().withRecords(RECORD));
        com.amazonaws.services.kinesis.model.GetRecordsResult result = adapterClient.getRecords(new GetRecordsRequest().withShardIterator(TEST_STRING));
        assertEquals(1, result.getRecords().size());
        verify(mockClient).getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class));
    }

    @Test
    public void testGetRecordsLimit() {
        when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).then(new Answer<GetRecordsResult>() {

            @Override
            public GetRecordsResult answer(InvocationOnMock invocation) throws Throwable {
                com.amazonaws.services.dynamodbv2.model.GetRecordsRequest request = invocation.getArgumentAt(0, com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class);
                assertEquals(LIMIT, request.getLimit().intValue());
                assertEquals(SHARD_ITERATOR, request.getShardIterator());
                return new GetRecordsResult().withRecords(RECORD);
            }

        });
        com.amazonaws.services.kinesis.model.GetRecordsResult result = adapterClient.getRecords(new GetRecordsRequest().withShardIterator(SHARD_ITERATOR).withLimit(LIMIT));
        assertEquals(1, result.getRecords().size());
        verify(mockClient).getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class));
    }

    @Test
    public void testGetRecordsLimitExceedDynamoDBStreams() {
        when(mockClient.getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).then(new Answer<GetRecordsResult>() {

            @Override
            public GetRecordsResult answer(InvocationOnMock invocation) throws Throwable {
                com.amazonaws.services.dynamodbv2.model.GetRecordsRequest request = invocation.getArgumentAt(0, com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class);
                assertEquals(AmazonDynamoDBStreamsAdapterClient.GET_RECORDS_LIMIT, request.getLimit());
                assertEquals(SHARD_ITERATOR, request.getShardIterator());
                return new GetRecordsResult().withRecords(RECORD);
            }

        });
        com.amazonaws.services.kinesis.model.GetRecordsResult result =
            adapterClient.getRecords(new GetRecordsRequest().withShardIterator(SHARD_ITERATOR).withLimit(KINESIS_GET_RECORDS_LIMIT));
        assertEquals(1, result.getRecords().size());
        verify(mockClient).getRecords(any(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSplitShard() {
        adapterClient.splitShard(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSplitShard2() {
        adapterClient.splitShard(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateStream() {
        adapterClient.createStream(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateStream2() {
        adapterClient.createStream(null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteStream() {
        adapterClient.deleteStream(TEST_STRING);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDeleteStream2() {
        adapterClient.deleteStream((DeleteStreamRequest) null);
    }

    @Test
    public void testListStreams() {
        final ListStreamsResult result = new ListStreamsResult().withStreams(STREAM);
        when(mockClient.listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class))).thenReturn(result);
        adapterClient.listStreams(new ListStreamsRequest());
        verify(mockClient).listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class));
    }

    @Test
    public void testListStreams2() {
        when(mockClient.listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class))).then(new Answer<ListStreamsResult>() {

            @Override
            public com.amazonaws.services.dynamodbv2.model.ListStreamsResult answer(InvocationOnMock invocation) throws Throwable {
                com.amazonaws.services.dynamodbv2.model.ListStreamsRequest request = invocation.getArgumentAt(0, com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class);
                assertEquals(STREAM_0000000, request.getExclusiveStartStreamArn());
                return new com.amazonaws.services.dynamodbv2.model.ListStreamsResult().withStreams(STREAM);
            }
        });
        com.amazonaws.services.kinesis.model.ListStreamsResult result = adapterClient.listStreams(STREAM_0000000);
        assertEquals(1, result.getStreamNames().size());
        assertEquals(STREAM.getStreamArn(), result.getStreamNames().get(0));
        verify(mockClient).listStreams(any(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeShards() {
        adapterClient.mergeShards(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMergeShards2() {
        adapterClient.mergeShards(null, null, null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddTagsToStream() {
        adapterClient.addTagsToStream(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testListTagsForStream() {
        adapterClient.listTagsForStream(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveTagsFromStream() {
        adapterClient.removeTagsFromStream(null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetSkipRecordsBehaviorNull() {
        adapterClient.setSkipRecordsBehavior(null);
    }

    @Test
    public void testDefaultSkipRecordsBehavior() {
        assertEquals(SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, adapterClient.getSkipRecordsBehavior());
    }

    @Test
    public void testShutdown() {
        adapterClient.shutdown();
        verify(mockClient).shutdown();
    }

    @Test
    public void testGetCachedResponseMetadata() {
        Map<String, String> responseHeaders = null;
        // Test DescribeStream
        ResponseMetadata expectedResponseMetadata = new ResponseMetadata(responseHeaders);
        when(mockClient.getCachedResponseMetadata(isNotNull(com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest.class))).thenReturn(expectedResponseMetadata);
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        adapterClient.describeStream(describeStreamRequest);
        ResponseMetadata actualResponseMetadata = adapterClient.getCachedResponseMetadata(describeStreamRequest);
        assertSameObject(expectedResponseMetadata, actualResponseMetadata);
        // Test GetRecords
        expectedResponseMetadata = new ResponseMetadata(responseHeaders);
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        when(mockClient.getCachedResponseMetadata(isNotNull(com.amazonaws.services.dynamodbv2.model.GetRecordsRequest.class))).thenReturn(expectedResponseMetadata);
        adapterClient.getRecords(getRecordsRequest);
        actualResponseMetadata = adapterClient.getCachedResponseMetadata(getRecordsRequest);
        assertSameObject(expectedResponseMetadata, actualResponseMetadata);
        // Test GetShardIterator
        expectedResponseMetadata = new ResponseMetadata(responseHeaders);
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        when(mockClient.getCachedResponseMetadata(isNotNull(com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest.class))).thenReturn(expectedResponseMetadata);
        adapterClient.getShardIterator(getShardIteratorRequest);
        actualResponseMetadata = adapterClient.getCachedResponseMetadata(getShardIteratorRequest);
        assertSameObject(expectedResponseMetadata, actualResponseMetadata);
        // Test ListStreams
        expectedResponseMetadata = new ResponseMetadata(responseHeaders);
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        when(mockClient.getCachedResponseMetadata(isNotNull(com.amazonaws.services.dynamodbv2.model.ListStreamsRequest.class))).thenReturn(expectedResponseMetadata);
        adapterClient.listStreams(listStreamsRequest);
        actualResponseMetadata = adapterClient.getCachedResponseMetadata(listStreamsRequest);
        assertSameObject(expectedResponseMetadata, actualResponseMetadata);
    }

    private static void assertSameObject(Object expected, Object actual) {
        int expectedId = System.identityHashCode(expected);
        int actualId = System.identityHashCode(actual);
        assertEquals(expectedId, actualId);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDescreaseStreamRetentionPeriod() {
        adapterClient.decreaseStreamRetentionPeriod(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIncreaseStreamRetentionPeriod() {
        adapterClient.increaseStreamRetentionPeriod(null);
    }

}
