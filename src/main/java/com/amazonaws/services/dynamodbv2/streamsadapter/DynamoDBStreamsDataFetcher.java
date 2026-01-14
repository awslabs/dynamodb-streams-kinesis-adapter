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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName;

import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsGetRecordsResponseAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.common.DynamoDBStreamsRequestsBuilder;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsCatchUpConfig;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.ShardFilter;
import software.amazon.awssdk.services.dynamodb.model.ShardFilterType;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.KinesisException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.AWSExceptionManager;
import software.amazon.kinesis.retrieval.DataFetcherProviderConfig;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.GetRecordsResponseAdapter;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.RetryableRetrievalException;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.polling.DataFetcher;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implements fetching data from DynamoDB Streams using GetRecords and GetShardIterator API.
 */
@Slf4j
public class DynamoDBStreamsDataFetcher implements DataFetcher {
    private static final String METRICS_PREFIX = "DynamoDBStreamsDataFetcher";
    private static final String OPERATION = "ProcessTask";

    protected static final int MAX_DESCRIBE_STREAM_ATTEMPTS_FOR_CHILD_SHARD_DISCOVERY_ON_NO_RECORDS = 10;
    private static final int DESCRIBE_STREAM_FOR_CHILD_SHARD_DISCOVERY_BACKOFF_ON_NO_RECORDS_MAX_DELAY_IN_MILLIS = 1000;
    private static final int DESCRIBE_STREAM_FOR_CHILD_SHARD_DISCOVERY_BACKOFF_ON_NO_RECORDS_BASE_DELAY_IN_MILLIS = 50;

    @NonNull
    private final DynamoDBStreamsCatchUpConfig catchUpConfig;

    @NonNull
    private final AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient;

    @NonNull
    @Getter
    private final StreamIdentifier streamIdentifier;

    @NonNull
    private final String shardId;
    private final int maxRecords;

    @NonNull
    private final MetricsFactory metricsFactory;
    private final String streamAndShardId;

    @Getter(AccessLevel.PACKAGE)
    private String nextIterator;

    @Getter
    private boolean isShardEndReached;

    @Getter
    private boolean isInitialized;

    @Getter
    private String lastKnownSequenceNumber;

    final Duration maxFutureWait;

    private InitialPositionInStreamExtended initialPositionInStream;

    private String consumerId;

    /**
     * Reusable {@link AWSExceptionManager}.
     * <p>
     * N.B. This instance is mutable, but thread-safe for <b>read-only</b> use.
     * </p>
     */
    private static final AWSExceptionManager AWS_EXCEPTION_MANAGER = createExceptionManager();

    private static AWSExceptionManager createExceptionManager() {
        final AWSExceptionManager exceptionManager = new AWSExceptionManager();
        exceptionManager.add(ResourceNotFoundException.class, t -> t);
        exceptionManager.add(KinesisException.class, t -> t);
        exceptionManager.add(SdkException.class, t -> t);
        return exceptionManager;
    }

    private static final int DEFAULT_MAX_RECORDS = 1000;

    final DataFetcherResult terminalResult = new DataFetcherResult() {

        @Override
        public GetRecordsResponseAdapter getResultAdapter() {
            return new DynamoDBStreamsGetRecordsResponseAdapter(
                    GetRecordsResponse.builder()
                            .records(Collections.emptyList())
                            .nextShardIterator(null)
                            .build()
            );
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getResult() {
            throw new UnsupportedOperationException("getResult not implemented for DynamoDBStreamsDataFetcher");
        }

        @Override
        public GetRecordsResponseAdapter acceptAdapter() {
            nextIterator = null;
            isShardEndReached = true;
            return getResultAdapter();
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse accept() {
            throw new UnsupportedOperationException("accept not implemented for DynamoDBStreamsDataFetcher");
        }

        @Override
        public boolean isShardEnd() {
            return true;
        }
    };

    public DynamoDBStreamsDataFetcher(@NotNull AmazonDynamoDBStreamsAdapterClient amazonDynamoDBStreamsAdapterClient,
                                      DataFetcherProviderConfig dynamoDBStreamsDataFetcherProviderConfig,
                                      @NotNull DynamoDBStreamsCatchUpConfig catchUpConfig) {
        this.amazonDynamoDBStreamsAdapterClient = amazonDynamoDBStreamsAdapterClient;
        this.catchUpConfig = catchUpConfig;
        this.maxRecords = Math.min(dynamoDBStreamsDataFetcherProviderConfig.getMaxRecords(), DEFAULT_MAX_RECORDS);
        this.metricsFactory = dynamoDBStreamsDataFetcherProviderConfig.getMetricsFactory();
        this.streamIdentifier = dynamoDBStreamsDataFetcherProviderConfig.getStreamIdentifier();
        this.shardId = dynamoDBStreamsDataFetcherProviderConfig.getShardId();
        this.streamAndShardId = String.format("%s:%s",
                KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(streamIdentifier.streamName()),
                shardId);
        this.maxFutureWait = dynamoDBStreamsDataFetcherProviderConfig.getKinesisRequestTimeout();
        this.consumerId = dynamoDBStreamsDataFetcherProviderConfig.consumerId();
    }

    /**
     * Call GetRecords and get records back from DynamoDB Streams.
     *
     * @return {@link DataFetcherResult} containing records and whether the shard has reached end or not.
     */
    @Override
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalStateException("DynamoDBStreamsDataFetcher.getRecords method called "
                    + "before initialization.");
        }

        if (nextIterator != null) {
            try {
                return new AdvancingResult(ddbGetRecords(nextIterator));
            } catch (ResourceNotFoundException e) {
                log.info("Caught ResourceNotFoundException when fetching" +
                        " records for stream and shard {}", streamAndShardId);
                return terminalResult;
            }
        } else {
            return terminalResult;
        }
    }

    /**
     * Initialize the data fetcher with an initial checkpoint.
     * @param initialCheckpoint       Current checkpoint sequence number for this shard.
     *                                For TRIM_HORIZON and LATEST, it will be TRIM_HORIZON and LATEST.
     *                                For everything else, it will be the last sequence number checkpointed in the
     *                                lease table.
     * @param initialPositionInStream The initial position in stream. Will be either TRIM_HORIZON or LATEST for
     *                                DynamoDB Streams.
     */
    @Override
    public void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        log.info("Initializing stream and shard: {} with: {}", streamAndShardId, initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    @Override
    public void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended
            initialPositionInStream) {
        log.info("Initializing stream and shard: {} with: {}", streamAndShardId, initialCheckpoint.sequenceNumber());
        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    /**
     * Advance the iterator to the given sequence number.
     *
     * @param sequenceNumber          advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    @Override
    public void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        }

        GetShardIteratorRequest.Builder getShardIteratorRequestBuilder = GetShardIteratorRequest
                .builder()
                .streamName(createDynamoDBStreamsArnFromKinesisStreamName(streamIdentifier.streamName()))
                .shardId(shardId)
                .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                        .addApiName(ApiName.builder()
                                .name(consumerId)
                                .version(RetrievalConfig.KINESIS_CLIENT_LIB_USER_AGENT_VERSION)
                                .build())
                        .build());

        if (Objects.equals(ExtendedSequenceNumber.LATEST.sequenceNumber(), sequenceNumber)) {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.LATEST);
        } else if (Objects.equals(ExtendedSequenceNumber.TRIM_HORIZON.sequenceNumber(), sequenceNumber)) {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.TRIM_HORIZON);
        } else if (Objects.equals(ExtendedSequenceNumber.SHARD_END.sequenceNumber(), sequenceNumber)) {
            nextIterator = null;
            isShardEndReached = true;
            this.lastKnownSequenceNumber = sequenceNumber;
            this.initialPositionInStream = initialPositionInStream;
            return;
        } else {
            getShardIteratorRequestBuilder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            getShardIteratorRequestBuilder.startingSequenceNumber(sequenceNumber);
        }
        GetShardIteratorRequest request = getShardIteratorRequestBuilder.build();
        log.debug("[GetShardIterator] Request has parameters {}", request);

        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();

        try {
            try {
                nextIterator = getNextIterator(request);
                success = true;
            } catch (ExecutionException e) {
                throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RetryableRetrievalException(e.getMessage(), e);
            }
        } catch (ResourceNotFoundException e) {
            log.info("Caught ResourceNotFoundException when getting an iterator" +
                    " for stream and shard {}", streamAndShardId, e);
            nextIterator = null;
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getShardIterator"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }

        if (nextIterator == null) {
            isShardEndReached = true;
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * Restart the iterator using AT_SEQUENCE_NUMBER call.
     */
    @Override
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalArgumentException("Make sure to initialize the DynamoDBStreamsDataFetcher"
                    + " before restarting the iterator.");
        }
        log.debug("Restarting iterator for sequence number {} on shard id {}",
                lastKnownSequenceNumber, streamAndShardId);
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
    }

    /**
     * Reset the iterator to the given sharditerator, sequence number and initial position.
     */
    @Override
    public void resetIterator(String shardIterator, String sequenceNumber,
                              InitialPositionInStreamExtended initialPositionInStream) {
        this.nextIterator = shardIterator;
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse
    getGetRecordsResponse(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest request) throws Exception {
        throw new UnsupportedOperationException("getGetRecordsResponse is " +
                "not implemented for DynamoDBStreamsDataFetcher");
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsRequest getGetRecordsRequest(String nextIterator) {
        throw new UnsupportedOperationException("getGetRecordsRequest is " +
                "not implemented for DynamoDBStreamsDataFetcher");
    }

    /**
     * Call GetRecords API of DynamoDB Streams and return the result.
     *
     * @param request the current get records request used to receive a response.
     * @return GetRecordsResponse.
     */
    public GetRecordsResponseAdapter getGetRecordsResponse(GetRecordsRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        DynamoDBStreamsGetRecordsResponseAdapter getRecordsResponseAdapter =
                amazonDynamoDBStreamsAdapterClient.getDynamoDBStreamsRecords(request).get();

        // We have reached the end of the shard, call DescribeStream API with ShardFilter parameter
        if (Objects.isNull(getRecordsResponseAdapter.nextShardIterator())) {
            DescribeStreamResponse describeStreamResponse = getChildShards(streamIdentifier.streamName(), shardId);
            if (describeStreamResponse != null) {
                List<ChildShard> childShards = describeStreamResponse.streamDescription()
                        .shards()
                        .stream()
                        .map(shard -> ChildShard.builder()
                                .shardId(shard.shardId())
                                .parentShards(
                                        Stream.of(shard.parentShardId(), shard.adjacentParentShardId())
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList()))
                                .hashKeyRange(shard.hashKeyRange())
                                .build()
                        )
                        .collect(Collectors.toList());
                getRecordsResponseAdapter.addChildShards(childShards);
            }
        }
        return getRecordsResponseAdapter;
    }

    @VisibleForTesting
    protected DescribeStreamResponse getChildShards(String streamName, String shardId) throws InterruptedException {
        int attempts = 0;
        do {
            try {
                DescribeStreamResponse describeStreamResponse = amazonDynamoDBStreamsAdapterClient
                        .describeStreamWithFilter(
                                createDynamoDBStreamsArnFromKinesisStreamName(streamName),
                                ShardFilter.builder()
                                        .type(ShardFilterType.CHILD_SHARDS)
                                        .shardId(shardId)
                                        .build(),
                                consumerId);

                if (!describeStreamResponse.streamDescription().shards().isEmpty()) {
                    return describeStreamResponse;
                }

                // if stream is disabled and no child shards are found, we will not retry for this case.
                if (StreamStatus.DISABLED.toString().equals(
                        describeStreamResponse.streamDescription().streamStatusAsString())) {
                    return null;
                }
            } catch (LimitExceededException e) {
                log.error("Caught limit exceeded exception while getting child shards for stream and shard: {}",
                        streamAndShardId, e);
            } catch (Exception e) {
                // if there is any exception, fall back to paginated DescribeStream call for shard discovery
                log.error("Caught exception while getting child shards from stream and shard: {}",
                        streamAndShardId, e);
                return null;
            }
            attempts++;
            // Calculate exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms, 1000ms, ...
            long delayMillis =
                    Math.min(DESCRIBE_STREAM_FOR_CHILD_SHARD_DISCOVERY_BACKOFF_ON_NO_RECORDS_BASE_DELAY_IN_MILLIS
                                    * (1L << attempts),
                            DESCRIBE_STREAM_FOR_CHILD_SHARD_DISCOVERY_BACKOFF_ON_NO_RECORDS_MAX_DELAY_IN_MILLIS);

            // Add jitter (±20% of delay)
            long jitter = (long) (delayMillis * 0.2 * (Math.random() - 0.5) * 2);
            delayMillis += jitter;
            Thread.sleep(delayMillis);
        } while (attempts < MAX_DESCRIBE_STREAM_ATTEMPTS_FOR_CHILD_SHARD_DISCOVERY_ON_NO_RECORDS);
        log.error("Finding child shards for stream and shard: {} failed after {} attempts", streamAndShardId, attempts);
        return null;
    }

    /**
     * Build the GetRecordsRequest with the given next iterator.
     *
     * @param nextIterator the next iterator to be used to build the GetRecordsRequest.
     * @return GetRecordsRequest.
     */
    public GetRecordsRequest ddbGetRecordsRequest(String nextIterator) {
        return DynamoDBStreamsRequestsBuilder.getRecordsRequestBuilder(consumerId, catchUpConfig)
                .shardIterator(nextIterator)
                .limit(maxRecords)
                .build();
    }

    /**
     * Get the next iterator using GetShardIterator API.
     * If the shard is closed, return null.
     * If the shard is not found, return null.
     * If the shard is trimmed, call GetShardIterator API with TRIM_HORIZON.
     *
     * @param request the current get shard iterator request used to receive a response.
     * @return the next iterator.
     */
    public String getNextIterator(GetShardIteratorRequest request) throws ExecutionException, InterruptedException,
            TimeoutException {
        final GetShardIteratorResponse result = amazonDynamoDBStreamsAdapterClient.getShardIterator(request).get();
        return result.shardIterator();
    }

    @Override
    public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getRecords(@NonNull String nextIterator) {
        throw new UnsupportedOperationException("getRecords is not implemented for DynamoDBStreamsDataFetcher");
    }

    public GetRecordsResponseAdapter ddbGetRecords(@NonNull String nextIterator) {
        GetRecordsRequest getRecordsRequest = ddbGetRecordsRequest(nextIterator);
        final MetricsScope metricsScope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        MetricsUtil.addStreamId(metricsScope, streamIdentifier);
        MetricsUtil.addShardId(metricsScope, shardId);
        boolean success = false;
        long startTime = System.currentTimeMillis();
        try {
            final GetRecordsResponseAdapter response = getGetRecordsResponse(getRecordsRequest);
            success = true;
            return response;
        } catch (ExecutionException e) {
            throw AWS_EXCEPTION_MANAGER.apply(e.getCause());
        } catch (InterruptedException e) {
            log.debug("{} : Interrupt called on method, shutdown initiated", streamAndShardId);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RetryableRetrievalException(e.getMessage(), e);
        } finally {
            MetricsUtil.addSuccessAndLatency(
                    metricsScope,
                    String.format("%s.%s", METRICS_PREFIX, "getRecords"),
                    success,
                    startTime,
                    MetricsLevel.DETAILED);
            MetricsUtil.endScope(metricsScope);
        }
    }

    @Data
    class AdvancingResult implements DataFetcherResult {
        final GetRecordsResponseAdapter result;

        @Override
        public GetRecordsResponseAdapter getResultAdapter() {
            return result;
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse getResult() {
            throw new UnsupportedOperationException("AdvancingResult.getResult is " +
                    "not implemented for DynamoDBStreamsDataFetcher");
        }

        @Override
        public GetRecordsResponseAdapter acceptAdapter() {
            nextIterator = result.nextShardIterator();
            if (CollectionUtils.isNotEmpty(result.records())) {
                lastKnownSequenceNumber = Iterables.getLast(result.records()).sequenceNumber();
            }
            if (nextIterator == null) {
                isShardEndReached = true;
            }
            return getResultAdapter();
        }

        @Override
        public software.amazon.awssdk.services.kinesis.model.GetRecordsResponse accept() {
            throw new UnsupportedOperationException("AdvancingResult.accept is " +
                    "not implemented for DynamoDBStreamsDataFetcher");
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }

    }
}
