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

import javax.annotation.Nullable;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.dynamodb.model.ExpiredIteratorException;
import software.amazon.awssdk.services.dynamodb.model.InternalServerErrorException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;

/**
 * <p>
 * This class transforms an Amazon DynamoDB Streams AmazonServiceException into a compatible Amazon Kinesis
 * AmazonServiceException.
 * </p>
 * Applicable API calls:
 * <p>
 * <ul>
 * <li>DescribeStream
 * <ul>
 * <li><a
 * href="http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_DescribeStream.html">Amazon
 * DynamoDB Streams</a></li>
 * <li><a href="http://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStream.html">Amazon Kinesis</a></li>
 * </ul>
 * </li>
 * <li>GetRecords
 * <ul>
 * <li><a href="http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetRecords.html">Amazon
 * DynamoDB Streams</a></li>
 * <li><a href="http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html">Amazon Kinesis</a></li>
 * </ul>
 * </li>
 * <li>GetShardIterator
 * <ul>
 * <li><a
 * href="http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html">Amazon
 * DynamoDB Streams</a></li>
 * <li><a href="http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html">Amazon Kinesis</a></li>
 * </ul>
 * </li>
 * <li>ListStreams
 * <ul>
 * <li><a href="http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_ListStreams.html">Amazon
 * DynamoDB Streams</a></li>
 * <li><a href="http://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListStreams.html">Amazon Kinesis</a></li>
 * </ul>
 * </li>
 * </ul>
 * </p>
 */
@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public final class AmazonServiceExceptionTransformer {

    private AmazonServiceExceptionTransformer() {}

    private static final String TRIMMED_DATA_KCL_RETRY_MESSAGE = "Attempted to get a shard iterator for a trimmed "
            + "shard. Data has been lost";

    /**
     * Error message used for ThrottlingException by the Amazon DynamoDB Streams service.
     */
    public static final String DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE = "ThrottlingException";

    /**
     * Empty String used when an Exception requires an error message, but none is supplied.
     */
    public static final String EMPTY_STRING = "";
    /**
     * Error message used for InternalFailure by the Amazon Kinesis service.
     */
    public static final String KINESIS_INTERNAL_ERROR_MESSAGE = "InternalFailure";
    /**
     * Error message used for ValidationError by the Amazon Kinesis service.
     */
    public static final String KINESIS_VALIDATION_ERROR_MESSAGE = "ValidationError";
    private static final Log LOG = LogFactory.getLog(AmazonServiceExceptionTransformer.class);

    /**
     * After determining the transformation in the API specific transform method, this helper method sets the
     * transformed exception's fields to match the original exception. If no transformation occurred, the original
     * exception is returned.
     *
     * @param original    The original DynamoDB Streams exception
     * @param transforming The equivalent Kinesis exception that needs its fields updated
     * @return The final result of the transformation ready to be thrown by the adapter
     */
    private static AwsServiceException applyFields(AwsServiceException original,
                                                   AwsServiceException.Builder transforming) {
        if (transforming == null) {
            LOG.error("Could not transform a DynamoDB AmazonServiceException to a compatible Kinesis exception",
                    original);
            return original;
        }
        AwsErrorDetails.Builder transformingDetailsBuilder = getDetailsBuilder(transforming.awsErrorDetails());
        // Here we update the transformed exception fields with the original exception values
        if (original.awsErrorDetails() != null && original.awsErrorDetails().errorCode() != null) {
            transformingDetailsBuilder.errorCode(original.awsErrorDetails().errorCode());
        }

        if (original.awsErrorDetails() != null && original.awsErrorDetails().serviceName() != null) {
            transformingDetailsBuilder.serviceName(original.awsErrorDetails().serviceName());
        }
        //Set anything set above to the transforming builder
        transforming.awsErrorDetails(transformingDetailsBuilder.build());

        if (original.requestId() != null) {
            transforming.requestId(original.requestId());
        }
        transforming.statusCode(original.statusCode());
        LOG.debug(String.format("DynamoDB Streams exception: %s tranformed to Kinesis %s", original.getClass(),
                transforming.getClass()), original);
        return transforming.build();
    }

    private static AwsErrorDetails.Builder getDetailsBuilder(@Nullable AwsErrorDetails aed) {
        return aed == null ? AwsErrorDetails.builder() : aed.toBuilder();
    }

    /**
     * Builds the error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     *
     * @param ase The original exception
     * @return The error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     */
    private static String buildErrorMessage(AwsServiceException ase) {
        if (ase.getMessage() == null) {
            return EMPTY_STRING;
        }
        return ase.getMessage();
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the DescribeStream API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link InternalServerErrorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.AmazonServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: N/A<br>
     * <br>
     * (2) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link AwsServiceException} with ErrorCode
     * {@value AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.LimitExceededException} <br>
     * Notes: N/A<br>
     *
     * @param ase The Amazon DynamoDB Streams exception thrown by a DescribeStream call
     * @return A compatible Amazon Kinesis DescribeStream exception
     */
    public static AwsServiceException transformDynamoDBStreamsToKinesisDescribeStream(AwsServiceException ase) {
        final AwsServiceException.Builder transforming;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof InternalServerErrorException) { // (1)
            transforming = AwsServiceException.builder().message(buildErrorMessage(ase)).cause(ase);
        } else if (ase instanceof ResourceNotFoundException) { // (2)
            transforming = software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder()
                    .message(buildErrorMessage(ase));
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.awsErrorDetails().errorCode())) { // (3)
            transforming = software.amazon.awssdk.services.kinesis.model.LimitExceededException.builder()
                    .message(buildErrorMessage(ase));
        } else {
            transforming = null;
        }
        return applyFields(ase, transforming);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the GetRecords API.
     * <p>
     * The following transformations are applied: <br>
     * (1) ExpiredIterator <br>
     * Amazon DynamoDB Streams: {@link ExpiredIteratorException} <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException} <br>
     * Notes: N/A <br>
     * <br>
     * (2) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link InternalServerErrorException} <br>
     * Amazon Kinesis: {@link AwsServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (3) LimitExceeded <br>
     * Amazon DynamoDB Streams: {@link LimitExceededException} <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * <br>
     * (4) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: N/A<br>
     * <br>
     * (5) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link AwsServiceException} with ErrorCode
     * {@value AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * (6) TrimmedDataAccess <br>
     * Amazon DynamoDB Streams: {@link software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException}<br>
     * Amazon Kinesis: N/A<br>
     * Notes: FIXME Amazon Kinesis does not communicate trimmed data; the service retrieves the oldest available records
     * for the shard and returns those as if no trimming occurred. Because no context information about the shardId or
     * streamId is available in the GetRecords request, the adapter relies on the user attempting to get a fresh shard
     * iterator using the GetShardIterator API when an ExpiredIteratorException is thrown. If data loss is unacceptable,
     * an {@link UnableToReadMoreRecordsException} is thrown.<br>
     *
     * @param ase                 The Amazon DynamoDB Streams exception thrown by a GetRecords call
     * @param skipRecordsBehavior The {@link AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior} for the adapter
     * @return A compatible Amazon Kinesis GetRecords exception
     */
    public static AwsServiceException transformDynamoDBStreamsToKinesisGetRecords(
            AwsServiceException ase, AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        final AwsServiceException.Builder transforming;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof ExpiredIteratorException) { // (1)
            transforming = software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException.builder()
                    .message(buildErrorMessage(ase));
        } else if (ase instanceof InternalServerErrorException) { // (2)
            transforming = AwsServiceException.builder().message(buildErrorMessage(ase)).cause(ase);
        } else if (ase instanceof LimitExceededException) { // (3)
            transforming = software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
                    .builder().message(buildErrorMessage(ase));
        } else if (ase instanceof ResourceNotFoundException) { // (4)
            transforming = software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder()
                    .message(buildErrorMessage(ase));
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.awsErrorDetails().errorCode())) { // (5)
            transforming = software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
                    .builder().message(buildErrorMessage(ase));
        } else if (ase instanceof TrimmedDataAccessException) { // (6)
            if (skipRecordsBehavior == AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior
                    .SKIP_RECORDS_TO_TRIM_HORIZON) { // Data loss is acceptable
                transforming = software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException.builder()
                        .message(buildErrorMessage(ase));
            } else { // Data loss is NOT acceptable
                throw new UnableToReadMoreRecordsException("Attempted to access trimmed data. Data has been lost", ase);
            }
        } else {
            transforming = null;
        }
        return applyFields(ase, transforming);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the GetShardIterator
     * API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link InternalServerErrorException} <br>
     * Amazon Kinesis: {@link AwsServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: Amazon Kinesis does not differentiate TrimmedData and ResourceNotFound. In the case that data loss is not
     * acceptable, the adapter throws an {@link UnableToReadMoreRecordsException}<br>
     * <br>
     * (3) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link AwsServiceException} with ErrorCode
     * {@value AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * (4) TrimmedDataAccess <br>
     * Amazon DynamoDB Streams: {@link TrimmedDataAccessException}<br>
     * Amazon Kinesis: N/A<br>
     * Notes: FIXME Amazon Kinesis does not communicate trimmed data; the service returns a valid shard iterator for the
     * oldest available records for the specified shard as if no trimming occurred. If data loss is acceptable, the
     * adapter mimics this behavior by intercepting the exception and retrieving a shard iterator for TRIM_HORIZON. If
     * data loss is unacceptable, an {@link UnableToReadMoreRecordsException} is thrown.<br>
     *
     * @param ase                 The Amazon DynamoDB Streams exception thrown by a GetRecords call
     * @param skipRecordsBehavior The {@link AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior} for the adapter
     * @return A compatible Amazon Kinesis GetRecords exception
     */
    public static AwsServiceException transformDynamoDBStreamsToKinesisGetShardIterator(
            AwsServiceException ase, AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior skipRecordsBehavior) {
        if (ase == null) {
            return ase;
        }
        AwsServiceException.Builder transforming = null;
        if (ase instanceof InternalServerErrorException) { // (1)
            transforming = AwsServiceException.builder().message(buildErrorMessage(ase)).cause(ase);
        } else if (ase instanceof ResourceNotFoundException) { // (2)
            if (skipRecordsBehavior == AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior
                    .SKIP_RECORDS_TO_TRIM_HORIZON) {
                transforming = software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder()
                        .message(buildErrorMessage(ase));
            } else {
                throw new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, ase);
            }
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.awsErrorDetails().errorCode())) { // (3)
            transforming = ProvisionedThroughputExceededException.builder().message(buildErrorMessage(ase));
        } else if (ase instanceof TrimmedDataAccessException) { // (4)
            if (skipRecordsBehavior == AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior
                    .SKIP_RECORDS_TO_TRIM_HORIZON) {
                transforming = software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException.builder()
                        .message(buildErrorMessage(ase));
            } else {
                throw new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, ase);
            }
        }
        return applyFields(ase, transforming);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the ListStreams API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link InternalServerErrorException} <br>
     * Amazon Kinesis: {@link AwsServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound<br>
     * Amazon DynamoDB Streams: {@link ResourceNotFoundException} <br>
     * Amazon Kinesis: N/A <br>
     * Notes: A compatible transformation is to an AmazonServiceException with a 400 series StatusCode<br>
     * <br>
     * (3) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link AwsServiceException} with ErrorCode
     * {@value AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link software.amazon.awssdk.services.kinesis.model.LimitExceededException} <br>
     * Notes: N/A<br>
     *
     * @param ase The Amazon DynamoDB Streams exception thrown by a ListStreams call
     * @return A compatible Amazon Kinesis ListStreams exception
     */
    public static AwsServiceException transformDynamoDBStreamsToKinesisListStreams(AwsServiceException ase) {
        final AwsServiceException.Builder transforming;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof InternalServerErrorException) { // (1)
            transforming = AwsServiceException.builder().message(buildErrorMessage(ase)).cause(ase);
        } else if (ase instanceof ResourceNotFoundException) { // (2)
            transforming = AwsServiceException.builder().message(buildErrorMessage(ase)).cause(ase);
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.awsErrorDetails().errorCode())) { // (3)
            transforming = software.amazon.awssdk.services.kinesis.model.LimitExceededException.builder()
                    .message(buildErrorMessage(ase));
        } else {
            transforming = null;
        }
        return applyFields(ase, transforming);
    }
}
