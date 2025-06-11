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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.AmazonServiceExceptionTransformer.DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE;

import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsGetRecordsResponseAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.common.DynamoDBStreamsRequestsBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.AmazonServiceExceptionTransformer;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TrimmedDataAccessException;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class AmazonDynamoDBStreamsAdapterClient implements KinesisAsyncClient {

    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind.
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record.
         */
        SKIP_RECORDS_TO_TRIM_HORIZON,
        /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data.
         */
        KCL_RETRY
    }

    @Getter
    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;
    private static final int MAX_DESCRIBE_STREAM_RETRY_ATTEMPTS = 50;
    private static final Duration DESCRIBE_STREAM_CALLS_DELAY = Duration.ofMillis(1000);

    private Region region;

    private final DynamoDbStreamsClient internalClient;
    private final Sleeper sleeper;

    /**
     * Recommended constructor for {@link AmazonDynamoDBStreamsAdapterClient} which takes in the aws credentials and
     * the region where the DynamoDB Stream will be consumed from.
     *
     * @param credentialsProvider AWS credentials provider
     * @param region AWS region for the client
     */
    public AmazonDynamoDBStreamsAdapterClient(AwsCredentialsProvider credentialsProvider,
                                              Region region) {
        BackoffStrategy backoffStrategy = BackoffStrategy.exponentialDelay(
                Duration.ofMillis(100), Duration.ofMillis(10000));
        StandardRetryStrategy retryStrategy =
                SdkDefaultRetryStrategy.standardRetryStrategy()
                        .toBuilder()
                        .maxAttempts(10)
                        .backoffStrategy(backoffStrategy)
                        .throttlingBackoffStrategy(backoffStrategy)
                        .build();
        internalClient = DynamoDbStreamsClient.builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryStrategy(retryStrategy)
                        .build())
                .region(region)
                .build();
        this.region = region;
        this.sleeper = new Sleeper();
    }

    /**
     * Constructor for {@link AmazonDynamoDBStreamsAdapterClient} in which a custom
     * {@link DynamoDbStreamsClient} can be passed.
     *
     * For optimal performance, DynamoDB Recommends the base throttling retry
     * delay to 100ms and the max delay to 10000ms.
     *
     * @param dynamoDbStreamsClient
     * @param region
     */
    public AmazonDynamoDBStreamsAdapterClient(
            DynamoDbStreamsClient dynamoDbStreamsClient,
            Region region) {
        internalClient = dynamoDbStreamsClient;
        this.region = region;
        this.sleeper = new Sleeper();
    }

    @VisibleForTesting
    protected AmazonDynamoDBStreamsAdapterClient(DynamoDbStreamsClient client) {
        this.internalClient = client;
        this.sleeper = new Sleeper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String serviceName() {
        return internalClient.serviceName();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        internalClient.close();
    }

    /**
     * Fetches all the available Shards for a stream using the provided request from the DynamoDB Streams.
     * @param describeStreamRequest Container for the necessary parameters to execute the DescribeStream service method
     *                             on DynamoDB Streams
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(
            software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest describeStreamRequest)
            throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            DescribeStreamRequest ddbDescribeStreamRequest =
                    DynamoDBStreamsRequestsBuilder.describeStreamRequestBuilder()
                            .streamArn(describeStreamRequest.streamName())
                            .limit(describeStreamRequest.limit())
                            .exclusiveStartShardId(describeStreamRequest.exclusiveStartShardId())
                            .build();

            software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse result;
            try {
                result = describeStreamWithRetries(ddbDescribeStreamRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(e);
            }
            return KinesisMapperUtil.convertDynamoDBDescribeStreamResponseToKinesisDescribeStreamResponse(result);
        });
    }

    /**
     * Gets a shard iterator using the provided request from DynamoDB Streams.
     * @param getShardIteratorRequest Container for the necessary parameters to execute the GetShardIterator service
     *                                method on DynamoDB Streams.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest)
            throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> getShardIteratorResponse(getShardIteratorRequest));
    }

    private GetShardIteratorResponse getShardIteratorResponse(GetShardIteratorRequest getShardIteratorRequest)
            throws AwsServiceException, SdkClientException {
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest ddbGetShardIteratorRequest =
                DynamoDBStreamsRequestsBuilder.getShardIteratorRequestBuilder()
                        .streamArn(getShardIteratorRequest.streamName())
                        .shardIteratorType(getShardIteratorRequest.shardIteratorTypeAsString())
                        .shardId(getShardIteratorRequest.shardId())
                        .sequenceNumber(getShardIteratorRequest.startingSequenceNumber())
                        .build();
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse result;
        try {
            result = internalClient.getShardIterator(ddbGetShardIteratorRequest);
        } catch (TrimmedDataAccessException e) {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                if (getShardIteratorRequest.shardIteratorType().equals(
                        software.amazon.awssdk.services.kinesis.model.ShardIteratorType.TRIM_HORIZON)) {
                    throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e,
                            skipRecordsBehavior);
                }
                log.warn("Data has been trimmed. Intercepting DynamoDB exception and retrieving a fresh iterator {}",
                        getShardIteratorRequest, e);
                return getShardIteratorResponse(
                        getShardIteratorRequest.toBuilder()
                                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                .startingSequenceNumber(null)
                                .build());
            } else {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e,
                        skipRecordsBehavior);
            }
        } catch (AwsServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e,
                    skipRecordsBehavior);
        }
        return KinesisMapperUtil.convertDynamoDBGetShardIteratorResponseToKinesisGetShardIteratorResponse(result);
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return KinesisServiceClientConfiguration.builder()
                .region(this.region)
                .build();
    }

    /**
     * Lists streams using the provided request from DynamoDB Streams.
     * @param listStreamsRequest Container for the necessary parameters to execute the ListStreams service method on
     *                           DynamoDB Streams.
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest)
            throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest ddbListStreamsRequest =
                    DynamoDBStreamsRequestsBuilder.listStreamsRequestBuilder()
                            .limit(listStreamsRequest.limit())
                            .exclusiveStartStreamArn(listStreamsRequest.exclusiveStartStreamName())
                            .build();
            try {
                software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse listStreamsResponse =
                        internalClient.listStreams(ddbListStreamsRequest);
                return KinesisMapperUtil.convertDynamoDBListStreamsResponseToKinesisListStreamsResponse(
                        listStreamsResponse);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisListStreams(
                        e);
            }
        });
    }

    /**
     * Gets records using the provided request .This method is not supported by the DynamoDB Streams adapter.
     * @param getRecordsRequest Container for the necessary parameters to execute the GetRecords service method on
     *                          DynamoDB Streams.
     * @return The response from the GetRecords service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest)
            throws AwsServiceException, SdkClientException {
        throw new UnsupportedOperationException("DDB Adapter does not implement kinesis getrecords."
                + " See getDynamoDBStreamsRecords function");
    }

    public CompletableFuture<GetRecordsResponseAdapter> getDynamoDBStreamsRecords(
            software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest ddbGetRecordsRequest
    ) throws AwsServiceException, SdkClientException {
        return CompletableFuture.supplyAsync(() -> {
            software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse result;
            try {
                result = internalClient.getRecords(ddbGetRecordsRequest);
            } catch (AwsServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(e,
                        skipRecordsBehavior);
            }
            return new DynamoDBStreamsGetRecordsResponseAdapter(result);
        });
    }

    /**
     * Sets a value of {@link SkipRecordsBehavior} to decide how the application handles the case when records are lost.
     * Default = {@link SkipRecordsBehavior#SKIP_RECORDS_TO_TRIM_HORIZON}
     *
     * @param skipRecordsBehavior A {@link SkipRecordsBehavior} for the adapter
     */
    public void setSkipRecordsBehavior(SkipRecordsBehavior skipRecordsBehavior) {
        if (skipRecordsBehavior == null) {
            throw new NullPointerException("skipRecordsBehavior cannot be null");
        }
        this.skipRecordsBehavior = skipRecordsBehavior;
    }

    private software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse describeStreamWithRetries(
            DescribeStreamRequest describeStreamRequest) {
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse describeStreamResponse = null;
        int remainingRetries = MAX_DESCRIBE_STREAM_RETRY_ATTEMPTS;
        AwsServiceException lastException = null;
        while (describeStreamResponse == null) {

            try {
                describeStreamResponse = internalClient.describeStream(describeStreamRequest);
            } catch (AwsServiceException e) {
                if (DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE.equals(e.awsErrorDetails().errorCode())) {
                    log.debug("Got LimitExceededException from DescribeStream, retrying {} times", remainingRetries);
                    sleeper.sleep(DESCRIBE_STREAM_CALLS_DELAY.toMillis());
                    lastException = e;
                }
                if (e instanceof ResourceNotFoundException) {
                    throw e;
                }
            }
            remainingRetries--;
            if (remainingRetries == 0 && describeStreamResponse == null) {
                if (lastException != null) {
                    throw lastException;

                }
                throw new IllegalStateException("Received null from DescribeStream call.");

            }

        }
        return describeStreamResponse;

    }
}
