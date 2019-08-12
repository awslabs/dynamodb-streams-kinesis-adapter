/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.LimitExceededException;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior;

@PrepareForTest({AmazonDynamoDBStreamsClient.class, AmazonDynamoDBStreamsAdapterClient.class})
@RunWith(PowerMockRunner.class)
public class AmazonServiceExceptionTransformerTests extends AmazonServiceExceptionTransformer {

    private static final AWSCredentials FAKE_CREDS = new BasicAWSCredentials("DummyAccessKey", "DummySecretKey");

    private static final String INTERNAL_FAILURE = "InternalFailure";

    private static final String LIMIT_EXCEEDED_EXCEPTION = "LimitExceededException";

    private static final String REQUEST_ID = "request-id-1";

    private static final String RESOURCE_NOT_FOUND = "ResourceNotFound";

    private static final String SEQUENCE_NUMBER = "Sequence-Number-000000000";

    private static final String SERVICE_NAME = "dynamodb";

    private static final String SHARD_ID = "ShardId";

    private static final String SHARD_ITERATOR = "shard iterator 1";

    private static final int STATUS_CODE_400 = 400;

    private static final int STATUS_CODE_500 = 500;

    private static final String STREAM_NAME = "StreamName";

    private static final String TEST_MESSAGE = "Test Message";

    private static final String THROTTLING_EXCEPTION = "ThrottlingException";

    private static final String TRIMMED_DATA_ACCESS_EXCEPTION = "TrimmedDataAccessException";

    /**
     * Checks that all AmazonServiceException properties are equal between the expected and actual exceptions.
     *
     * @param expected Exception with expected properties
     * @param actual   Exception generated during the test
     */
    private static void assertSameExceptionProperties(AmazonServiceException expected, AmazonServiceException actual) {
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.getErrorCode(), actual.getErrorCode());
        if (expected.getErrorMessage() == null && actual.getErrorMessage() != null) {
            assertEquals(AmazonServiceExceptionTransformer.EMPTY_STRING, actual.getErrorMessage());
        } else {
            assertEquals(expected.getErrorMessage(), actual.getErrorMessage());
        }
        assertEquals(expected.getErrorType(), actual.getErrorType());
        assertEquals(expected.getRequestId(), actual.getRequestId());
        assertEquals(expected.getServiceName(), actual.getServiceName());
        assertEquals(expected.getStatusCode(), actual.getStatusCode());
    }

    /**
     * Helper function to set AmazonServiceException properties.
     *
     * @param ase         The Exception to modify
     * @param errorCode   Error code property
     * @param errorType   Error type property
     * @param requestId   RequestId property
     * @param serviceName ServiceName property
     * @param statusCode  StatusCode property
     */
    private static void setFields(AmazonServiceException ase, String errorCode, ErrorType errorType, String requestId, String serviceName, int statusCode) {
        if (errorCode != null) {
            ase.setErrorCode(errorCode);
        }
        if (errorType != null) {
            ase.setErrorType(errorType);
        }
        if (requestId != null) {
            ase.setRequestId(requestId);
        }
        if (serviceName != null) {
            ase.setServiceName(serviceName);
        }
        ase.setStatusCode(statusCode);
    }

    private final AmazonDynamoDBStreamsClient streams = PowerMockito.mock(AmazonDynamoDBStreamsClient.class);

    private void doDescribeStreamTest(AmazonServiceException ase, Class<?> expectedResult) throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withAnyArguments().thenReturn(streams);
        when(streams.describeStream(Matchers.any(DescribeStreamRequest.class))).thenThrow(ase);
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(FAKE_CREDS);
        try {
            adapterClient.describeStream(STREAM_NAME);
            fail("Expected " + expectedResult.getCanonicalName());
        } catch (AmazonServiceException e) {
            assertEquals(expectedResult, e.getClass());
            assertSameExceptionProperties(ase, e);
        }
        verify(streams, Mockito.times(1)).describeStream(Matchers.any(DescribeStreamRequest.class));
    }

    private void doGetRecordsTest(AmazonServiceException ase, Class<?> expectedResult, SkipRecordsBehavior skipRecordsBehavior) throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withAnyArguments().thenReturn(streams);
        when(streams.getRecords(Matchers.any(GetRecordsRequest.class))).thenThrow(ase);
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(FAKE_CREDS);
        adapterClient.setSkipRecordsBehavior(skipRecordsBehavior);
        try {
            adapterClient.getRecords(new com.amazonaws.services.kinesis.model.GetRecordsRequest().withShardIterator(SHARD_ITERATOR));
            fail("Expected " + expectedResult.getCanonicalName());
        } catch (RuntimeException e) {
            assertEquals(expectedResult, e.getClass());
            if (e instanceof AmazonServiceException) {
                assertSameExceptionProperties(ase, (AmazonServiceException) e);
            }
        }
        verify(streams, Mockito.times(1)).getRecords(Matchers.any(GetRecordsRequest.class));
    }

    private void doGetShardIteratorTest(AmazonServiceException ase, Class<?> expectedResult, SkipRecordsBehavior skipRecordsBehavior, int numCalls) throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withAnyArguments().thenReturn(streams);
        when(streams.getShardIterator(Matchers.any(GetShardIteratorRequest.class))).thenThrow(ase);
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(FAKE_CREDS);
        adapterClient.setSkipRecordsBehavior(skipRecordsBehavior);
        try {
            adapterClient.getShardIterator(STREAM_NAME, SHARD_ID, ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), SEQUENCE_NUMBER);
            fail("Expected " + expectedResult.getCanonicalName());
        } catch (RuntimeException e) {
            assertEquals(expectedResult, e.getClass());
            if (e instanceof AmazonServiceException) {
                assertSameExceptionProperties(ase, (AmazonServiceException) e);
            }
        }
        verify(streams, Mockito.times(numCalls)).getShardIterator(Matchers.any(GetShardIteratorRequest.class));
    }

    private void doListStreamsTest(AmazonServiceException ase, Class<?> expectedResult) throws Exception {
        whenNew(AmazonDynamoDBStreamsClient.class).withAnyArguments().thenReturn(streams);
        when(streams.listStreams(Matchers.any(ListStreamsRequest.class))).thenThrow(ase);
        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(FAKE_CREDS);
        try {
            adapterClient.listStreams();
            fail("Expected " + expectedResult.getCanonicalName());
        } catch (AmazonServiceException e) {
            assertEquals(expectedResult, e.getClass());
            assertSameExceptionProperties(ase, e);
        }
        verify(streams, Mockito.times(1)).listStreams(Matchers.any(ListStreamsRequest.class));
    }

    @Test()
    public void testDescribeStreamInternalServerErrorException() throws Exception {
        InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
        setFields(ise, INTERNAL_FAILURE, ErrorType.Service, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
        doDescribeStreamTest(ise, com.amazonaws.AmazonServiceException.class);
    }

    @Test
    public void testDescribeStreamIrrelevantException() throws Exception {
        // Not thrown by DynamoDB Streams
        ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
        setFields(exception, null, null, null, null, STATUS_CODE_400);
        doDescribeStreamTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class);
    }

    @Test()
    public void testDescribeStreamResourceNotFoundException() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doDescribeStreamTest(rnfe, com.amazonaws.services.kinesis.model.ResourceNotFoundException.class);
    }

    @Test()
    public void testDescribeStreamThrottlingException() throws Exception {
        AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
        setFields(te, THROTTLING_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doDescribeStreamTest(te, com.amazonaws.services.kinesis.model.LimitExceededException.class);
    }

    @Test
    public void testExceptionWithNullFields() throws Exception {
        InternalServerErrorException ise = new InternalServerErrorException(null);
        setFields(ise, null, null, null, null, STATUS_CODE_500);
        doListStreamsTest(ise, com.amazonaws.AmazonServiceException.class);
    }

    @Test()
    public void testGetRecordsExpiredIteratorException() throws Exception {
        ExpiredIteratorException eie = new ExpiredIteratorException(TEST_MESSAGE);
        setFields(eie, INTERNAL_FAILURE, ErrorType.Service, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(eie, com.amazonaws.services.kinesis.model.ExpiredIteratorException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test()
    public void testGetRecordsInternalServerErrorException() throws Exception {
        InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
        setFields(ise, INTERNAL_FAILURE, ErrorType.Service, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
        doGetRecordsTest(ise, com.amazonaws.AmazonServiceException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test
    public void testGetRecordsIrrelevantException() throws Exception {
        // Not thrown by DynamoDB Streams
        ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
        setFields(exception, null, null, null, null, STATUS_CODE_400);
        doGetRecordsTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test()
    public void testGetRecordsLimitExceededException() throws Exception {
        LimitExceededException te = new LimitExceededException(TEST_MESSAGE);
        setFields(te, LIMIT_EXCEEDED_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(te, com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test()
    public void testGetRecordsResourceNotFoundExceptionKCLRetry() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(rnfe, com.amazonaws.services.kinesis.model.ResourceNotFoundException.class, SkipRecordsBehavior.KCL_RETRY);
    }

    @Test()
    public void testGetRecordsResourceNotFoundExceptionSkipRecords() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(rnfe, com.amazonaws.services.kinesis.model.ResourceNotFoundException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test()
    public void testGetRecordsThrottlingException() throws Exception {
        AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
        setFields(te, THROTTLING_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(te, com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test()
    public void testGetRecordsTrimmedDataAccessExceptionKCLRetry() throws Exception {
        TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
        setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(tdae, com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException.class, SkipRecordsBehavior.KCL_RETRY);
    }

    @Test()
    public void testGetRecordsTrimmedDataAccessExceptionSkipRecords() throws Exception {
        TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
        setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetRecordsTest(tdae, com.amazonaws.services.kinesis.model.ExpiredIteratorException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);
    }

    @Test
    public void testGetShardIteratorInternalServerErrorException() throws Exception {
        InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
        setFields(ise, INTERNAL_FAILURE, ErrorType.Service, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
        doGetShardIteratorTest(ise, com.amazonaws.AmazonServiceException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
    }

    @Test
    public void testGetShardIteratorIrrelevantException() throws Exception {
        // Not thrown by DynamoDB Streams
        ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
        setFields(exception, null, null, null, null, STATUS_CODE_400);
        doGetShardIteratorTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON,
            1);
    }

    @Test()
    public void testGetShardIteratorResourceNotFoundExceptionKCLRetry() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetShardIteratorTest(rnfe, com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException.class, SkipRecordsBehavior.KCL_RETRY, 1);
    }

    @Test()
    public void testGetShardIteratorResourceNotFoundExceptionSkipRecords() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetShardIteratorTest(rnfe, com.amazonaws.services.kinesis.model.ResourceNotFoundException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
    }

    @Test()
    public void testGetShardIteratorThrottlingException() throws Exception {
        AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
        setFields(te, THROTTLING_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetShardIteratorTest(te, com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 1);
    }

    @Test()
    public void testGetShardIteratorTrimmedDataAccessExceptionKCLRetry() throws Exception {
        TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
        setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetShardIteratorTest(tdae, com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException.class, SkipRecordsBehavior.KCL_RETRY, 1);
    }

    @Test()
    public void testGetShardIteratorTrimmedDataAccessExceptionSkipRecords() throws Exception {
        TrimmedDataAccessException tdae = new TrimmedDataAccessException(TEST_MESSAGE);
        setFields(tdae, TRIMMED_DATA_ACCESS_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doGetShardIteratorTest(tdae, com.amazonaws.services.kinesis.model.ResourceNotFoundException.class, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON, 2);
    }

    @Test
    public void testListStreamsInternalServerErrorException() throws Exception {
        InternalServerErrorException ise = new InternalServerErrorException(TEST_MESSAGE);
        setFields(ise, INTERNAL_FAILURE, ErrorType.Service, REQUEST_ID, SERVICE_NAME, STATUS_CODE_500);
        doListStreamsTest(ise, com.amazonaws.AmazonServiceException.class);
    }

    @Test
    public void testListStreamsIrrelevantException() throws Exception {
        // Not thrown by DynamoDB Streams
        ProvisionedThroughputExceededException exception = new ProvisionedThroughputExceededException(null);
        setFields(exception, null, null, null, null, STATUS_CODE_400);
        doListStreamsTest(exception, com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException.class);
    }

    @Test
    public void testListStreamsResourceNotFoundException() throws Exception {
        ResourceNotFoundException rnfe = new ResourceNotFoundException(TEST_MESSAGE);
        setFields(rnfe, RESOURCE_NOT_FOUND, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doListStreamsTest(rnfe, com.amazonaws.AmazonServiceException.class);
    }

    @Test
    public void testListStreamsThrottlingException() throws Exception {
        AmazonServiceException te = new AmazonServiceException(TEST_MESSAGE);
        setFields(te, THROTTLING_EXCEPTION, ErrorType.Client, REQUEST_ID, SERVICE_NAME, STATUS_CODE_400);
        doListStreamsTest(te, com.amazonaws.services.kinesis.model.LimitExceededException.class);
    }

    @Test
    public void testNullException() {
        // If exception is null, we should get back null
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisListStreams(null));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(null));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(null, SkipRecordsBehavior.KCL_RETRY));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(null, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(null, null));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(null, SkipRecordsBehavior.KCL_RETRY));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(null, SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON));
        assertEquals(null, AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(null, null));
    }
}
