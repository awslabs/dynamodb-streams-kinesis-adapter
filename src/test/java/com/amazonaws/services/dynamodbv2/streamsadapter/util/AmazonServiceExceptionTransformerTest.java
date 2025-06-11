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
package com.amazonaws.services.dynamodbv2.streamsadapter.util;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtils.TEST_ERROR_MESSAGE;
import static com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtils.TEST_REQUEST_ID;
import static com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtils.createThrottlingException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AmazonServiceExceptionTransformerTest {

    @Test
    void testTransformDescribeStreamInternalServerError() {
        InternalServerErrorException original = createException(
                InternalServerErrorException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisDescribeStream(original);

        assertNotNull(transformed);
        assertEquals(original.statusCode(), transformed.statusCode());
    }

    @Test
    void testTransformDescribeStreamResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisDescribeStream(original);

        assertNotNull(transformed);
        assertInstanceOf(software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.class, transformed);
    }

    @Test
    void testTransformDescribeStreamThrottling() {
        AwsServiceException original = createThrottlingException();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisDescribeStream(original);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.LimitExceededException);
    }

    @Test
    void testTransformGetRecordsExpiredIterator() {
        ExpiredIteratorException original = createException(
                ExpiredIteratorException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetRecords(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException);
    }

    @Test
    void testTransformGetRecordsInternalServerError() {
        InternalServerErrorException original = createException(
                InternalServerErrorException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetRecords(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
    }

    @Test
    void testTransformGetRecordsLimitExceeded() {
        LimitExceededException original = createException(
                LimitExceededException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetRecords(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertTrue(transformed instanceof ProvisionedThroughputExceededException);
    }

    @Test
    void testTransformGetRecordsTrimmedDataWithSkip() {
        TrimmedDataAccessException original = createException(
                TrimmedDataAccessException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetRecords(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException);
    }

    @Test
    void testTransformGetRecordsTrimmedDataWithRetry() {
        TrimmedDataAccessException original = createException(
                TrimmedDataAccessException.builder(), TEST_ERROR_MESSAGE).build();

        assertThrows(UnableToReadMoreRecordsException.class,
                () -> AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY));
    }

    @Test
    void testTransformGetShardIteratorResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetShardIterator(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException);
    }

    @Test
    void testTransformGetShardIteratorTrimmedDataWithSkip() {
        TrimmedDataAccessException original = createException(
                TrimmedDataAccessException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisGetShardIterator(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException);
    }

    @Test
    void testTransformGetShardIteratorTrimmedDataWithRetry() {
        TrimmedDataAccessException original = createException(
                TrimmedDataAccessException.builder(), TEST_ERROR_MESSAGE).build();

        assertThrows(UnableToReadMoreRecordsException.class,
                () -> AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(original,
                        AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior.KCL_RETRY));
    }

    @Test
    void testTransformListStreamsInternalServerError() {
        InternalServerErrorException original = createException(
                InternalServerErrorException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisListStreams(original);

        assertNotNull(transformed);
    }

    @Test
    void testTransformListStreamsResourceNotFound() {
        ResourceNotFoundException original = createException(
                ResourceNotFoundException.builder(), TEST_ERROR_MESSAGE).build();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisListStreams(original);

        assertNotNull(transformed);
    }

    @Test
    void testTransformListStreamsThrottling() {
        AwsServiceException original = createThrottlingException();

        AwsServiceException transformed = AmazonServiceExceptionTransformer
                .transformDynamoDBStreamsToKinesisListStreams(original);

        assertNotNull(transformed);
        assertTrue(transformed instanceof software.amazon.awssdk.services.kinesis.model.LimitExceededException);
    }

    private <T extends AwsServiceException.Builder> T createException(T builder, String message) {
        builder.message(message)
                .requestId(TEST_REQUEST_ID)
                .statusCode(500)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorMessage(message)
                        .serviceName("DynamoDB")
                        .build());
        return builder;
    }
}