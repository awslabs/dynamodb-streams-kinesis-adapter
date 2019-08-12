/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.http.AmazonHttpClient;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.TrimmedDataAccessException;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DescribeStreamRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DescribeStreamResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetRecordsRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetRecordsResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetShardIteratorRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetShardIteratorResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.ListStreamsRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.ListStreamsResultAdapter;
import com.amazonaws.services.kinesis.AbstractAmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.AddTagsToStreamResult;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamResult;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.DecreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamResult;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodRequest;
import com.amazonaws.services.kinesis.model.IncreaseStreamRetentionPeriodResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.MergeShardsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamResult;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.SplitShardRequest;
import com.amazonaws.services.kinesis.model.SplitShardResult;

/**
 * Client for accessing DynamoDB Streams using the Amazon Kinesis interface.
 */
public class AmazonDynamoDBStreamsAdapterClient extends AbstractAmazonKinesis {

    /**
     * Request cache capacity based on {@link AmazonHttpClient}.
     */
    private static final int REQUEST_CACHE_CAPACITY = 50;

    private static final Log LOG = LogFactory.getLog(AmazonDynamoDBStreamsAdapterClient.class);

    /* The maximum number of records to return in a single getRecords call. */
    public static final Integer GET_RECORDS_LIMIT = 1000;

    private final AmazonDynamoDBStreams internalClient;

    private final AdapterRequestCache requestCache = new AdapterRequestCache(REQUEST_CACHE_CAPACITY);


    /**
     * Enum values decides the behavior of application when customer loses some records when KCL lags behind
     */
    public enum SkipRecordsBehavior {
        /**
         * Skips processing to the oldest available record
         */
        SKIP_RECORDS_TO_TRIM_HORIZON, /**
         * Throws an exception to KCL, which retries (infinitely) to fetch the data
         */
        KCL_RETRY;
    }


    private SkipRecordsBehavior skipRecordsBehavior = SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON;

    /**
     * Whether or not to generate the ByteBuffer returned by
     * RecordAdapter::getData().  KCL uses the bytes returned by getData to
     * generate throughput metrics.  If these metrics are not needed then
     * choosing to not generate this data results in memory and CPU savings.
     * If this value is true then the data will be generated and KCL will generate
     * a correct throughput metric.  If this is false, getData()
     * will return an empty ByteBuffer and the KCL metric will always be zero.
     */
    private boolean generateRecordBytes = true;

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     */
    public AmazonDynamoDBStreamsAdapterClient() {
        internalClient = new AmazonDynamoDBStreamsClient();
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param clientConfiguration The client configuration options controlling how this client connects to DynamoDB Streams (ex: proxy
     *                            settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param awsCredentials The AWS credentials (access key ID and secret key) to use when authenticating with AWS services.
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentials awsCredentials) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentials);
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param awsCredentials      The AWS credentials (access key ID and secret key) to use when authenticating with AWS services.
     * @param clientConfiguration The client configuration options controlling how this client connects to DynamoDB Streams (ex: proxy
     *                            settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentials, clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *                               services.
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider);
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *                               services.
     * @param clientConfiguration    The client configuration options controlling how this client connects to DynamoDB Streams (ex: proxy
     *                               settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials to authenticate requests with AWS
     *                               services.
     * @param clientConfiguration    The client configuration options controlling how this client connects to DynamoDB Streams (ex: proxy
     *                               settings, retry counts, etc.).
     * @param requestMetricCollector Optional request metric collector
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration,
        RequestMetricCollector requestMetricCollector) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider, clientConfiguration, requestMetricCollector);
    }

    /**
     * Recommended constructor for AmazonDynamoDBStreamsAdapterClient which takes in an AmazonDynamoDBStreams
     * interface. If you need to execute setEndpoint(String,String,String) or setServiceNameIntern() methods,
     * you should do that on AmazonDynamoDBStreamsClient implementation before passing it in this constructor.
     *
     * @param amazonDynamoDBStreams The DynamoDB Streams to be used internally
     */
    public AmazonDynamoDBStreamsAdapterClient(AmazonDynamoDBStreams amazonDynamoDBStreams) {
        internalClient = amazonDynamoDBStreams;
    }

    /**
     * Overrides the default endpoint for this client. Callers can use this method to control which AWS region they want
     * to work with.
     *
     * @param endpoint The region specific AWS endpoint this client will communicate with.
     */
    @Override
    public void setEndpoint(String endpoint) {
        internalClient.setEndpoint(endpoint);
    }

    /**
     * Sets the regional endpoint for this client's service calls. Callers can use this method to control which AWS
     * region they want to work with.
     */
    @Override
    public void setRegion(Region region) {
        internalClient.setRegion(region);
    }

    /**
     * Determines RecordAdapter behavior when RecordAdapter::getData() is called.  This method should
     * usually be called immediately after instantiating this client.  If this method is called from
     * one thread while a GetRecords call is in progress on another thread then the behavior will be
     * non-deterministic.
     *
     * @param generateRecordBytes Whether or not to generate the ByteBuffer returned by
     *                            RecordAdapter::getData().  KCL uses the bytes returned by getData to
     *                            generate throughput metrics.  If these metrics are not needed then
     *                            choosing to not generate this data results in memory and CPU savings.
     *                            If this value is true then the data will be generated and KCL will generate
     *                            a correct throughput metric.  If this is false, getData()
     *                            will return an empty ByteBuffer and the KCL metric will always be zero.
     */
    public void setGenerateRecordBytes(boolean generateRecordBytes) {
        this.generateRecordBytes = generateRecordBytes;
    }

    /**
     * @param describeStreamRequest Container for the necessary parameters to execute the DescribeStream service method on DynamoDB
     *                              Streams.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        DescribeStreamRequestAdapter requestAdapter = new DescribeStreamRequestAdapter(describeStreamRequest);
        com.amazonaws.services.dynamodbv2.model.DescribeStreamResult result;
        requestCache.addEntry(describeStreamRequest, requestAdapter);
        try {
            result = internalClient.describeStream(requestAdapter);
        } catch (AmazonServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(e);
        }
        if (result.getStreamDescription().getStreamStatus().equals("DISABLED")) {
            // FIXME short-term solution
            // KCL does not currently support the concept of disabled streams. If there
            // are no active shards (i.e. EndingSequenceNumber not null), then KCL will
            // not create leases and will not process any shards in that stream. As a
            // short-term solution, we feign active shards by setting the ending
            // sequence number of all leaf nodes in the shard tree to null.
            List<Shard> allShards = getAllShardsForDisabledStream(result);
            markLeafShardsAsActive(allShards);
            StreamDescription newStreamDescription =
                new StreamDescription().withShards(allShards).withLastEvaluatedShardId(null).withCreationRequestDateTime(result.getStreamDescription().getCreationRequestDateTime())
                    .withKeySchema(result.getStreamDescription().getKeySchema()).withStreamArn(result.getStreamDescription().getStreamArn())
                    .withStreamLabel(result.getStreamDescription().getStreamLabel()).withStreamStatus(result.getStreamDescription().getStreamStatus())
                    .withTableName(result.getStreamDescription().getTableName()).withStreamViewType(result.getStreamDescription().getStreamViewType());
            result = new com.amazonaws.services.dynamodbv2.model.DescribeStreamResult().withStreamDescription(newStreamDescription);
        }
        return new DescribeStreamResultAdapter(result);
    }

    private List<Shard> getAllShardsForDisabledStream(com.amazonaws.services.dynamodbv2.model.DescribeStreamResult initialResult) {
        List<Shard> shards = new ArrayList<Shard>();
        shards.addAll(initialResult.getStreamDescription().getShards());

        com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest request;
        com.amazonaws.services.dynamodbv2.model.DescribeStreamResult result = initialResult;
        // Allowing KCL to paginate calls will not allow us to correctly determine the
        // leaf nodes. In order to avoid pagination issues when feigning shard activity, we collect all
        // shards in the adapter and return them at once.
        while (result.getStreamDescription().getLastEvaluatedShardId() != null) {
            request = new com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest().withStreamArn(result.getStreamDescription().getStreamArn())
                .withExclusiveStartShardId(result.getStreamDescription().getLastEvaluatedShardId());
            try {
                result = internalClient.describeStream(request);
            } catch (AmazonServiceException e) {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(e);
            }
            shards.addAll(result.getStreamDescription().getShards());
        }
        return shards;
    }

    private void markLeafShardsAsActive(List<Shard> shards) {
        List<String> parentShardIds = new ArrayList<String>();
        for (Shard shard : shards) {
            if (shard.getParentShardId() != null) {
                parentShardIds.add(shard.getParentShardId());
            }
        }
        for (Shard shard : shards) {
            // Feign shard activity by modifying leaf shards
            if (!parentShardIds.contains(shard.getShardId())) {
                shard.getSequenceNumberRange().setEndingSequenceNumber(null);
            }
        }
    }

    /**
     * @param streamName The ID of the stream to describe.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public DescribeStreamResult describeStream(String streamName) {
        return this.describeStream(streamName, null/* limit */, null/* exclusiveStartShardId */);
    }

    /**
     * @param streamName            The ID of the stream to describe.
     * @param exclusiveStartShardId The shard ID of the shard to start with for the stream description.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        return this.describeStream(streamName, null/* limit */, exclusiveStartShardId);
    }

    /**
     * @param streamName            The ID of the stream to describe.
     * @param limit                 The maximum number of shards to return.
     * @param exclusiveStartShardId The shard ID of the shard to start with for the stream description.
     * @return The response from the DescribeStream service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public DescribeStreamResult describeStream(String streamName, Integer limit, String exclusiveStartShardId) {
        DescribeStreamRequest request = new DescribeStreamRequest();
        request.setStreamName(streamName);
        request.setLimit(limit);
        request.setExclusiveStartShardId(exclusiveStartShardId);
        return this.describeStream(request);
    }

    /**
     * @param getShardIteratorRequest Container for the necessary parameters to execute the GetShardIterator service method on DynamoDB
     *                                Streams.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        GetShardIteratorRequestAdapter requestAdapter = new GetShardIteratorRequestAdapter(getShardIteratorRequest);
        requestCache.addEntry(getShardIteratorRequest, requestAdapter);

        try {
            com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult result = internalClient.getShardIterator(requestAdapter);
            return new GetShardIteratorResultAdapter(result);
        } catch (TrimmedDataAccessException e) {
            if (skipRecordsBehavior == SkipRecordsBehavior.SKIP_RECORDS_TO_TRIM_HORIZON) {
                if (getShardIteratorRequest.getShardIteratorType().equals(ShardIteratorType.TRIM_HORIZON.toString())) {
                    throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
                }
                LOG.warn(String.format("Data has been trimmed. Intercepting DynamoDB exception and retrieving a fresh iterator %s", getShardIteratorRequest), e);
                getShardIteratorRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON);
                getShardIteratorRequest.setStartingSequenceNumber(null);
                return getShardIterator(getShardIteratorRequest);
            } else {
                throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
            }
        } catch (AmazonServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetShardIterator(e, skipRecordsBehavior);
        }
    }

    /**
     * @param streamName        The ID of the stream.
     * @param shardId           The shard ID of the shard to get the iterator for
     * @param shardIteratorType Determines how the shard iterator is used to start reading data records from the shard.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType) {
        return this.getShardIterator(streamName, shardId, shardIteratorType, null/* startingSequenceNumber */);
    }

    /**
     * @param streamName             The ID of the stream.
     * @param shardId                The shard ID of the shard to get the iterator for
     * @param shardIteratorType      Determines how the shard iterator is used to start reading data records from the shard.
     * @param startingSequenceNumber The sequence number of the data record in the shard from which to start reading from.
     * @return The response from the GetShardIterator service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType, String startingSequenceNumber) {
        GetShardIteratorRequest request = new GetShardIteratorRequest();
        request.setStreamName(streamName);
        request.setShardId(shardId);
        request.setShardIteratorType(shardIteratorType);
        request.setStartingSequenceNumber(startingSequenceNumber);
        return this.getShardIterator(request);
    }

    // Not supported by the underlying Streams model
    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public PutRecordResult putRecord(String streamName, java.nio.ByteBuffer data, String partitionKey) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public PutRecordResult putRecord(String streamName, java.nio.ByteBuffer data, String partitionKey, String sequenceNumberForOrdering) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param getRecordsRequest Container for the necessary parameters to execute the GetRecords service method on DynamoDB Streams.
     * @return The response from the GetRecords service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        if (getRecordsRequest.getLimit() != null && getRecordsRequest.getLimit() > GET_RECORDS_LIMIT) {
            getRecordsRequest.setLimit(GET_RECORDS_LIMIT);
        }
        GetRecordsRequestAdapter requestAdapter = new GetRecordsRequestAdapter(getRecordsRequest);
        requestCache.addEntry(getRecordsRequest, requestAdapter);
        try {
            com.amazonaws.services.dynamodbv2.model.GetRecordsResult result = internalClient.getRecords(requestAdapter);
            return new GetRecordsResultAdapter(result, generateRecordBytes);
        } catch (AmazonServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisGetRecords(e, skipRecordsBehavior);
        }
    }

    // Not supported by the underlying Streams model
    @Override
    public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public SplitShardResult splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public CreateStreamResult createStream(String streamName, Integer shardCount) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public DeleteStreamResult deleteStream(String streamName) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param listStreamsRequest Container for the necessary parameters to execute the ListStreams service method on DynamoDB Streams.
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        ListStreamsRequestAdapter requestAdapter = new ListStreamsRequestAdapter(listStreamsRequest);
        requestCache.addEntry(listStreamsRequest, requestAdapter);
        try {
            com.amazonaws.services.dynamodbv2.model.ListStreamsResult result = internalClient.listStreams(requestAdapter);
            return new ListStreamsResultAdapter(result);
        } catch (AmazonServiceException e) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisListStreams(e);
        }

    }

    /**
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public ListStreamsResult listStreams() {
        return this.listStreams(null/* limit */, null/* exclusiveStartStreamName */);
    }

    /**
     * @param exclusiveStartStreamName The name of the stream to start the list with.
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public ListStreamsResult listStreams(String exclusiveStartStreamName) {
        return this.listStreams(null/* limit */, exclusiveStartStreamName);
    }

    /**
     * @param limit                    The maximum number of streams to list.
     * @param exclusiveStartStreamName The name of the stream to start the list with.
     * @return The response from the ListStreams service method, adapted for use with the AmazonKinesis model.
     */
    @Override
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
        ListStreamsRequest request = new ListStreamsRequest();
        request.setLimit(limit);
        request.setExclusiveStartStreamName(exclusiveStartStreamName);
        return this.listStreams(request);
    }

    // Not supported by the underlying Streams model
    @Override
    public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public MergeShardsResult mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public AddTagsToStreamResult addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public RemoveTagsFromStreamResult removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        throw new UnsupportedOperationException();
    }

    /**
     * Shuts down this client object, releasing any resources that might be held open. This is an optional method, and
     * callers are not expected to call it, but can if they want to explicitly release any open resources. Once a client
     * has been shutdown, it should not be used to make any more requests.
     */
    @Override
    public void shutdown() {
        internalClient.shutdown();
    }

    /**
     * @param request The originally executed request.
     * @return The response metadata for the specified request, or null if none is available.
     */
    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return internalClient.getCachedResponseMetadata(requestCache.getEntry(request));
    }

    /**
     * Gets the value of {@link SkipRecordsBehavior}.
     *
     * @return The value of {@link SkipRecordsBehavior}
     */
    public SkipRecordsBehavior getSkipRecordsBehavior() {
        return skipRecordsBehavior;
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

    // Not supported by the underlying Streams model
    @Override
    public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    @Override
    public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException();
    }
}
