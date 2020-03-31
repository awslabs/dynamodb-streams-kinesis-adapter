/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient.SkipRecordsBehavior;
import com.amazonaws.services.dynamodbv2.streamsadapter.exceptions.UnableToReadMoreRecordsException;

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
public class AmazonServiceExceptionTransformer {

    private static final String TRIMMED_DATA_KCL_RETRY_MESSAGE = "Attempted to get a shard iterator for a trimmed shard. Data has been lost";

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
     * @param transformed The equivalent Kinesis exception that needs its fields updated
     * @return The final result of the transformation ready to be thrown by the adapter
     */
    private static AmazonServiceException applyFields(AmazonServiceException original, AmazonServiceException transformed) {
        if (transformed == null) {
            LOG.error("Could not transform a DynamoDB AmazonServiceException to a compatible Kinesis exception", original);
            return original;
        }
        // Here we update the transformed exception fields with the original exception values
        if (original.getErrorCode() != null) {
            transformed.setErrorCode(original.getErrorCode());
        }
        // Null is transformed to UNKNOWN, so a null value is impossible.
        transformed.setErrorType(original.getErrorType());
        if (original.getRequestId() != null) {
            transformed.setRequestId(original.getRequestId());
        }
        if (original.getServiceName() != null) {
            transformed.setServiceName(original.getServiceName());
        }
        transformed.setStatusCode(original.getStatusCode());
        LOG.error(String.format("DynamoDB Streams exception: %s tranformed to Kinesis %s", original.getClass(), transformed.getClass()), original);
        return transformed;
    }

    /**
     * Builds the error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     *
     * @param ase The original exception
     * @return The error message for a transformed exception. Returns the original error message or an empty String if
     * the original error message was null.
     */
    private static String buildErrorMessage(AmazonServiceException ase) {
        if (ase.getErrorMessage() == null) {
            return EMPTY_STRING;
        }
        return ase.getErrorMessage();
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the DescribeStream API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.InternalServerErrorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.AmazonServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: N/A<br>
     * <br>
     * (2) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.AmazonServiceException} with ErrorCode
     * {@value com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.LimitExceededException} <br>
     * Notes: N/A<br>
     *
     * @param ase The Amazon DynamoDB Streams exception thrown by a DescribeStream call
     * @return A compatible Amazon Kinesis DescribeStream exception
     */
    public static AmazonServiceException transformDynamoDBStreamsToKinesisDescribeStream(AmazonServiceException ase) {
        final AmazonServiceException transformed;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof InternalServerErrorException) { // (1)
            transformed = new AmazonServiceException(buildErrorMessage(ase), ase);
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException) { // (2)
            transformed = new com.amazonaws.services.kinesis.model.ResourceNotFoundException(buildErrorMessage(ase));
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.getErrorCode())) { // (3)
            transformed = new com.amazonaws.services.kinesis.model.LimitExceededException(buildErrorMessage(ase));
        } else {
            transformed = null;
        }
        return applyFields(ase, transformed);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the GetRecords API.
     * <p>
     * The following transformations are applied: <br>
     * (1) ExpiredIterator <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ExpiredIteratorException} <br>
     * Notes: N/A <br>
     * <br>
     * (2) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.InternalServerErrorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.AmazonServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (3) LimitExceeded <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.LimitExceededException} <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * <br>
     * (4) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: N/A<br>
     * <br>
     * (5) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.AmazonServiceException} with ErrorCode
     * {@value com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * (6) TrimmedDataAccess <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException}<br>
     * Amazon Kinesis: N/A<br>
     * Notes: FIXME Amazon Kinesis does not communicate trimmed data; the service retrieves the oldest available records
     * for the shard and returns those as if no trimming occurred. Because no context information about the shardId or
     * streamId is available in the GetRecords request, the adapter relies on the user attempting to get a fresh shard
     * iterator using the GetShardIterator API when an ExpiredIteratorException is thrown. If data loss is unacceptable,
     * an {@link UnableToReadMoreRecordsException} is thrown.<br>
     *
     * @param ase                 The Amazon DynamoDB Streams exception thrown by a GetRecords call
     * @param skipRecordsBehavior The {@link SkipRecordsBehavior} for the adapter
     * @return A compatible Amazon Kinesis GetRecords exception
     */
    public static AmazonServiceException transformDynamoDBStreamsToKinesisGetRecords(AmazonServiceException ase, SkipRecordsBehavior skipRecordsBehavior) {
        final AmazonServiceException transformed;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof com.amazonaws.services.dynamodbv2.model.ExpiredIteratorException) { // (1)
            transformed = new com.amazonaws.services.kinesis.model.ExpiredIteratorException(buildErrorMessage(ase));
        } else if (ase instanceof InternalServerErrorException) { // (2)
            transformed = new AmazonServiceException(buildErrorMessage(ase), ase);
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.LimitExceededException) { // (3)
            transformed = new com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException(buildErrorMessage(ase));
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException) { // (4)
            transformed = new com.amazonaws.services.kinesis.model.ResourceNotFoundException(buildErrorMessage(ase));
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.getErrorCode())) { // (5)
            transformed = new com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException(buildErrorMessage(ase));
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException) { // (6)
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) { // Data loss is acceptable
                transformed = new com.amazonaws.services.kinesis.model.ExpiredIteratorException(buildErrorMessage(ase));
            } else { // Data loss is NOT acceptable
                throw new UnableToReadMoreRecordsException("Attempted to access trimmed data. Data has been lost", ase);
            }
        } else {
            transformed = null;
        }
        return applyFields(ase, transformed);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the GetShardIterator
     * API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.InternalServerErrorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.AmazonServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException} <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ResourceNotFoundException} <br>
     * Notes: Amazon Kinesis does not differentiate TrimmedData and ResourceNotFound. In the case that data loss is not
     * acceptable, the adapter throws an {@link UnableToReadMoreRecordsException}<br>
     * <br>
     * (3) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.AmazonServiceException} with ErrorCode
     * {@value com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException} <br>
     * Notes: N/A<br>
     * (4) TrimmedDataAccess <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException}<br>
     * Amazon Kinesis: N/A<br>
     * Notes: FIXME Amazon Kinesis does not communicate trimmed data; the service returns a valid shard iterator for the
     * oldest available records for the specified shard as if no trimming occurred. If data loss is acceptable, the
     * adapter mimics this behavior by intercepting the exception and retrieving a shard iterator for TRIM_HORIZON. If
     * data loss is unacceptable, an {@link UnableToReadMoreRecordsException} is thrown.<br>
     *
     * @param ase                 The Amazon DynamoDB Streams exception thrown by a GetRecords call
     * @param skipRecordsBehavior The {@link SkipRecordsBehavior} for the adapter
     * @return A compatible Amazon Kinesis GetRecords exception
     */
    public static AmazonServiceException transformDynamoDBStreamsToKinesisGetShardIterator(AmazonServiceException ase, SkipRecordsBehavior skipRecordsBehavior) {
        final AmazonServiceException transformed;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof InternalServerErrorException) { // (1)
            transformed = new AmazonServiceException(buildErrorMessage(ase), ase);
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException) { // (2)
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                transformed = new com.amazonaws.services.kinesis.model.ResourceNotFoundException(buildErrorMessage(ase));
            } else {
                throw new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, ase);
            }
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.getErrorCode())) { // (3)
            transformed = new com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException(buildErrorMessage(ase));
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException) { // (4)
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                transformed = new com.amazonaws.services.kinesis.model.ResourceNotFoundException(buildErrorMessage(ase));
            } else {
                throw new UnableToReadMoreRecordsException(TRIMMED_DATA_KCL_RETRY_MESSAGE, ase);
            }
        } else {
            transformed = null;
        }
        return applyFields(ase, transformed);
    }

    /**
     * Transforms Amazon DynamoDB Streams exceptions to compatible Amazon Kinesis exceptions for the ListStreams API.
     * <p>
     * The following transformations are applied: <br>
     * (1) InternalServerError <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.InternalServerErrorException} <br>
     * Amazon Kinesis: {@link com.amazonaws.AmazonServiceException} <br>
     * Notes: SDK relies on the 500 series StatusCode to identify that the issue was service side<br>
     * <br>
     * (2) ResourceNotFound<br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException} <br>
     * Amazon Kinesis: N/A <br>
     * Notes: A compatible transformation is to an AmazonServiceException with a 400 series StatusCode<br>
     * <br>
     * (3) ThrottlingException <br>
     * Amazon DynamoDB Streams: {@link com.amazonaws.AmazonServiceException} with ErrorCode
     * {@value com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer#DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE}
     * <br>
     * Amazon Kinesis: {@link com.amazonaws.services.kinesis.model.LimitExceededException} <br>
     * Notes: N/A<br>
     *
     * @param ase The Amazon DynamoDB Streams exception thrown by a ListStreams call
     * @return A compatible Amazon Kinesis ListStreams exception
     */
    public static AmazonServiceException transformDynamoDBStreamsToKinesisListStreams(AmazonServiceException ase) {
        final AmazonServiceException transformed;
        if (ase == null) {
            return ase;
        }
        if (ase instanceof InternalServerErrorException) { // (1)
            transformed = new AmazonServiceException(buildErrorMessage(ase), ase);
        } else if (ase instanceof com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException) { // (2)
            transformed = new AmazonServiceException(buildErrorMessage(ase), ase);
        } else if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(ase.getErrorCode())) { // (3)
            transformed = new com.amazonaws.services.kinesis.model.LimitExceededException(buildErrorMessage(ase));
        } else {
            transformed = null;
        }
        return applyFields(ase, transformed);
    }

}
