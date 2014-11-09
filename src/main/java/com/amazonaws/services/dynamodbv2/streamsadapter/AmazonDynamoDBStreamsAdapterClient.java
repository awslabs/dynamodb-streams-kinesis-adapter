/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.AddTagsToStreamRequest;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ListTagsForStreamRequest;
import com.amazonaws.services.kinesis.model.ListTagsForStreamResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.RemoveTagsFromStreamRequest;
import com.amazonaws.services.kinesis.model.SplitShardRequest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DescribeStreamRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DescribeStreamResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetRecordsRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetRecordsResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetShardIteratorRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.GetShardIteratorResultAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.ListStreamsRequestAdapter;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.ListStreamsResultAdapter;

/**
 * Client for accessing DynamoDB Streams using the
 * Amazon Kinesis interface.
 */
public class AmazonDynamoDBStreamsAdapterClient implements AmazonKinesis {
    
    private static final Log LOG = LogFactory.getLog(AmazonDynamoDBStreamsAdapterClient.class);
    private AmazonDynamoDBStreamsClient internalClient;

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     */
    public AmazonDynamoDBStreamsAdapterClient() {
        internalClient = new AmazonDynamoDBStreamsClient();
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param clientConfiguration The client configuration options controlling how this
     *                      client connects to DynamoDB Streams
     *                      (ex: proxy settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param awsCredentials The AWS credentials (access key ID and secret key) to use
     *                      when authenticating with AWS services.
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentials awsCredentials) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentials);
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param awsCredentials The AWS credentials (access key ID and secret key) to use
     *                      when authenticating with AWS services.
     * @param clientConfiguration The client configuration options controlling how this
     *                      client connects to DynamoDB Streams
     *                      (ex: proxy settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentials awsCredentials, ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentials, clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials
     *                      to authenticate requests with AWS services.
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider);
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials
     *                      to authenticate requests with AWS services.
     * @param clientConfiguration The client configuration options controlling how this
     *                      client connects to DynamoDB Streams
     *                      (ex: proxy settings, retry counts, etc.).
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider, ClientConfiguration clientConfiguration) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider, clientConfiguration);
    }

    /**
     * Constructs a new client to invoke service methods on
     * DynamoDB Streams.
     *
     * @param awsCredentialsProvider The AWS credentials provider which will provide credentials
     *                      to authenticate requests with AWS services.
     * @param clientConfiguration The client configuration options controlling how this
     *                      client connects to DynamoDB Streams
     *                      (ex: proxy settings, retry counts, etc.).
     * @param requestMetricCollector Optional request metric collector
     */
    public AmazonDynamoDBStreamsAdapterClient(AWSCredentialsProvider awsCredentialsProvider,
            ClientConfiguration clientConfiguration,
            RequestMetricCollector requestMetricCollector) {
        internalClient = new AmazonDynamoDBStreamsClient(awsCredentialsProvider, clientConfiguration, requestMetricCollector);
    }

    /*
     * Constructor used for testing.
     */
    public AmazonDynamoDBStreamsAdapterClient(AmazonDynamoDBStreamsClient client) {
        internalClient = client;
    }

    /**
     * Overrides the default endpoint for this client.
     * Callers can use this method to control which AWS region they want to work with.
     *
     * @param endpoint The region specific AWS endpoint this client will communicate with.
     */
    public void setEndpoint(String endpoint) {
        internalClient.setEndpoint(endpoint);
    }
    
    /**
     * Overrides the default endpoint for this client.
     * Callers can use this method to control which AWS region they want to work with.
     *
     * @param endpoint The region specific AWS endpoint this client will communicate with.
     */
    public void setEndpoint(String endpoint, String serviceName, String regionId) {
        internalClient.setEndpoint(endpoint, serviceName, regionId);
    }

    /**
     * An internal method used to explicitly override the service name
     * computed by the default implementation. This method is not expected to be
     * normally called except for AWS internal development purposes.
     */
    public void setServiceNameIntern(String serviceName) {
        internalClient.setServiceNameIntern(serviceName);
    }

    public void setRegion(Region region) {
        internalClient.setRegion(region);
    }

    /**
     * @param describeStreamRequest Container for the necessary parameters to
     *          execute the DescribeStream service method on DynamoDB Streams.
     * @return The response from the DescribeStream service method, adapted
     *          for use with the AmazonKinesis model.
     */
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        DescribeStreamRequestAdapter requestAdapter = new DescribeStreamRequestAdapter(describeStreamRequest);
        com.amazonaws.services.dynamodbv2.model.DescribeStreamResult result =
                internalClient.describeStream(requestAdapter);
        return new DescribeStreamResultAdapter(result);
    }

    /**
     * @param streamName The ID of the stream to describe.
     * @return The response from the DescribeStream service method, adapted
     *          for use with the AmazonKinesis model.
     */
    public DescribeStreamResult describeStream(String streamName) {
        return this.describeStream(streamName, new Integer(1000)/*default limit for streams*/, null/*exclusiveStartShardId*/);
    }

    /**
     * @param streamName The ID of the stream to describe.
     * @param exclusiveStartShardId The shard ID of the shard to start with
     *          for the stream description.
     * @return The response from the DescribeStream service method, adapted
     *          for use with the AmazonKinesis model.
     */
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        return this.describeStream(streamName, new Integer(1000)/*default limit for streams*/, exclusiveStartShardId);
    }

    /**
     * @param streamName The ID of the stream to describe.
     * @param limit The maximum number of shards to return.
     * @param exclusiveStartShardId The shard ID of the shard to start with
     *          for the stream description.
     * @return The response from the DescribeStream service method, adapted
     *          for use with the AmazonKinesis model.
     */
    public DescribeStreamResult describeStream(String streamName, Integer limit, String exclusiveStartShardId) {
        DescribeStreamRequest request = new DescribeStreamRequest();
        request.setStreamName(streamName);
        request.setLimit(limit);
        request.setExclusiveStartShardId(exclusiveStartShardId);
        return this.describeStream(request);
    }

    /**
     * @param getShardIteratorRequest Container for the necessary parameters
     *              to execute the GetShardIterator service method on DynamoDB Streams.
     * @return The response from the GetShardIterator service method, adapted
     *              for use with the AmazonKinesis model.
     */
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        GetShardIteratorRequestAdapter requestAdapter = new GetShardIteratorRequestAdapter(getShardIteratorRequest);
        com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult result =
                internalClient.getShardIterator(requestAdapter);
        return new GetShardIteratorResultAdapter(result);
    }

    /**
     * @param streamName The ID of the stream.
     * @param shardId The shard ID of the shard to get the iterator for
     * @param shardIteratorType Determines how the shard iterator is used to
     *              start reading data records from the shard.
     * @return The response from the GetShardIterator service method, adapted
     *              for use with the AmazonKinesis model.
     */
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType) {
        return this.getShardIterator(streamName, shardId, shardIteratorType, null/*startingSequenceNumber*/);
    }

    /**
     * @param streamName The ID of the stream.
     * @param shardId The shard ID of the shard to get the iterator for
     * @param shardIteratorType Determines how the shard iterator is used to
     *              start reading data records from the shard.
     * @param startingSequenceNumber The sequence number of the data record
     *              in the shard from which to start reading from.
     * @return The response from the GetShardIterator service method, adapted
     *              for use with the AmazonKinesis model.
     */
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType, String startingSequenceNumber) {
        GetShardIteratorRequest request = new GetShardIteratorRequest();
        request.setStreamName(streamName);
        request.setShardId(shardId);
        request.setShardIteratorType(shardIteratorType);
        request.setStartingSequenceNumber(startingSequenceNumber);
        return this.getShardIterator(request);
    }

    // Not supported by the underlying Streams model
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public PutRecordResult putRecord(String streamName, java.nio.ByteBuffer data, String partitionKey) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public PutRecordResult putRecord(String streamName, java.nio.ByteBuffer data, String partitionKey, String sequenceNumberForOrdering) {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @param getRecordsRequest Container for the necessary parameters to
     *          execute the GetRecords service method on DynamoDB Streams.
     * @return The response from the GetRecords service method, adapted for use
     *          with the AmazonKinesis model.
     */
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        if (getRecordsRequest.getLimit() != null && getRecordsRequest.getLimit() > 1000) {
            getRecordsRequest.setLimit(1000);
        }
        GetRecordsRequestAdapter requestAdapter = new GetRecordsRequestAdapter(getRecordsRequest);
        com.amazonaws.services.dynamodbv2.model.GetRecordsResult result =
                internalClient.getRecords(requestAdapter);
        return new GetRecordsResultAdapter(result);
    }

    // Not supported by the underlying Streams model
    public void splitShard(SplitShardRequest splitShardRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void createStream(CreateStreamRequest createStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void createStream(String streamName, Integer shardCount) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void deleteStream(DeleteStreamRequest deleteStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void deleteStream(String streamName) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param listStreamsRequest Container for the necessary parameters to
     *          execute the ListStreams service method on DynamoDB Streams.
     * @return The response from the ListStreams service method, adapted for use with
     *          the AmazonKinesis model.
     */
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        ListStreamsRequestAdapter requestAdapter = new ListStreamsRequestAdapter(listStreamsRequest);
        com.amazonaws.services.dynamodbv2.model.ListStreamsResult result =
                internalClient.listStreams(requestAdapter);
        return new ListStreamsResultAdapter(result);
    }

    /**
     * @return The response from the ListStreams service method, adapted for use with
     *          the AmazonKinesis model.
     */
    public ListStreamsResult listStreams() {
        return this.listStreams(new Integer(1000)/*default limit for streams*/, null/*exclusiveStartStreamName*/);
    }

    /**
     * @param exclusiveStartStreamName The name of the stream to start the list with.
     * @return The response from the ListStreams service method, adapted for use with
     *          the AmazonKinesis model.
     */
    public ListStreamsResult listStreams(String exclusiveStartStreamName) {
        return this.listStreams(new Integer(1000)/*default limit for streams*/, exclusiveStartStreamName);
    }

    /**
     * @param limit The maximum number of streams to list.
     * @param exclusiveStartStreamName The name of the stream to start the list with.
     * @return The response from the ListStreams service method, adapted for use with
     *          the AmazonKinesis model.
     */
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
        ListStreamsRequest request = new ListStreamsRequest();
        request.setLimit(limit);
        request.setExclusiveStartStreamName(exclusiveStartStreamName);
        return this.listStreams(request);
    }

    // Not supported by the underlying Streams model
    public void mergeShards(MergeShardsRequest mergeShardsRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        throw new UnsupportedOperationException();
    }

    // Not supported by the underlying Streams model
    public void removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        throw new UnsupportedOperationException();
    }

    /**
     * Shuts down this client object, releasing any resources that might be held
     * open. This is an optional method, and callers are not expected to call
     * it, but can if they want to explicitly release any open resources. Once a
     * client has been shutdown, it should not be used to make any more
     * requests.
     */
    public void shutdown() {
        internalClient.shutdown();
    }

    // Not supported by the underlying Streams model
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new UnsupportedOperationException();
    }

}
