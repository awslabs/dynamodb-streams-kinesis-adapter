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

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.leases.MultiStreamLease;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

public class TestUtils {

    public static final String TEST_ERROR_MESSAGE = "Test error message";
    public static final String TEST_REQUEST_ID = "test-request-id";
    /**
     * Creates a Kinesis-style shard for testing purposes.
     *
     * @param shardId              The ID of the shard
     * @param parentShardId        The parent shard ID (can be null)
     * @param adjacentParentShardId The adjacent parent shard ID (can be null)
     * @return A configured Shard object
     */
    public static Shard createTestShard(String shardId, String parentShardId, String adjacentParentShardId) {
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .adjacentParentShardId(adjacentParentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber("0")
                                .endingSequenceNumber(null)
                                .build()
                )
                .hashKeyRange(
                        HashKeyRange.builder()
                                .startingHashKey("0")
                                .endingHashKey("99999")
                                .build()
                )
                .build();
    }

    /**
     * Creates a shard for testing purposes.
     *
     * @param shardId       The ID of the shard
     * @param parentShardId The parent shard ID (can be null)
     * @param startSeq      The starting sequence number
     * @param endSeq       The ending sequence number (can be null for open shards)
     * @return A configured DynamoDB Shard object
     */
    public static Shard createShard(
            String shardId,
            String parentShardId,
            String startSeq,
            String endSeq) {
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber(startSeq)
                                .endingSequenceNumber(endSeq)
                                .build()
                )
                .build();
    }

    /**
     * Creates a MultiStreamLease object for testing purposes.
     *
     * @param streamArn  The ARN of the stream
     * @param shardId    The ID of the shard
     * @param checkpoint The checkpoint sequence number
     * @return A configured MultiStreamLease object
     */
    public static MultiStreamLease createMultiStreamLease(String streamArn,
                                                          String shardId,
                                                          ExtendedSequenceNumber checkpoint) {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey(MultiStreamLease.getLeaseKey(streamArn, shardId));
        lease.streamIdentifier(streamArn);
        lease.shardId(shardId);
        lease.checkpoint(checkpoint);
        return lease;
    }

    /**
     * Creates a DynamoDB stream shard with custom hash key range.
     *
     * @param shardId       The ID of the shard
     * @param parentShardId The parent shard ID
     * @param startHash     The starting hash key
     * @param endHash      The ending hash key
     * @return A configured Shard object
     */
    public static Shard createShardWithHashRange(String shardId,
                                                 String parentShardId,
                                                 String startHash,
                                                 String endHash) {
        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber("0")
                                .build()
                )
                .hashKeyRange(
                        HashKeyRange.builder()
                                .startingHashKey(startHash)
                                .endingHashKey(endHash)
                                .build()
                )
                .build();
    }

    /**
     * Creates a closed DynamoDB shard (with ending sequence number).
     *
     * @param shardId       The ID of the shard
     * @param parentShardId The parent shard ID
     * @param startSeq      The starting sequence number
     * @param endSeq       The ending sequence number
     * @return A configured closed Shard object
     */
    public static software.amazon.awssdk.services.dynamodb.model.Shard createDynamoDBShard(
            String shardId,
            String parentShardId,
            String startSeq,
            String endSeq) {
        return software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
                                .startingSequenceNumber(startSeq)
                                .endingSequenceNumber(endSeq)
                                .build()
                )
                .build();
    }

    /**
     * Creates a describe stream result with the given stream status.
     *
     * @param streamStatus The status of the stream
     * @param shards      The list of shards in the stream
     * @return A configured DescribeStreamResult object
     */
    public static DescribeStreamResult createDescribeStreamResult(
            String streamStatus,
            List<Shard> shards) {
        DescribeStreamResult result = new DescribeStreamResult();
        result.setStreamStatus(streamStatus);
        result.addShards(shards);
        return result;
    }

    /**
     * Creates a shard with specified sequence number range
     *
     * @param shardId The unique identifier for the shard
     * @param parentShardId The parent shard's ID
     * @param adjacentParentShardId The adjacent parent shard's ID
     * @param startingSequenceNumber The starting sequence number for this shard
     * @param endingSequenceNumber The ending sequence number for this shard (null for open shards)
     * @return A DynamoDB Shard object
     */
    public static Shard createTestShard(
            String shardId,
            String parentShardId,
            String adjacentParentShardId,
            String startingSequenceNumber,
            String endingSequenceNumber) {

        return Shard.builder()
                .shardId(shardId)
                .parentShardId(parentShardId)
                .sequenceNumberRange(
                        SequenceNumberRange.builder()
                                .startingSequenceNumber(startingSequenceNumber)
                                .endingSequenceNumber(endingSequenceNumber)
                                .build()
                )
                .hashKeyRange(
                        HashKeyRange.builder()
                                .startingHashKey("0")
                                .endingHashKey("1")
                                .build()
                )
                .build();
    }

    /**
     * Creates a completed (SHARD_END) lease for testing.
     *
     * @param streamArn The ARN of the stream
     * @param shardId The ID of the shard
     * @param parentShardId The parent shard ID (can be null)
     * @return A completed MultiStreamLease
     */
    public static MultiStreamLease createCompletedLease(
            String streamArn,
            String shardId,
            String parentShardId) {
        return createTestLease(
                streamArn,
                shardId,
                ExtendedSequenceNumber.SHARD_END,
                parentShardId
        );
    }

    /**
     * Creates an active lease with a specific sequence number for testing.
     *
     * @param streamArn The ARN of the stream
     * @param shardId The ID of the shard
     * @param sequenceNumber The current sequence number
     * @param parentShardId The parent shard ID (can be null)
     * @return An active MultiStreamLease
     */
    public static MultiStreamLease createActiveLease(
            String streamArn,
            String shardId,
            String sequenceNumber,
            String parentShardId) {
        return createTestLease(
                streamArn,
                shardId,
                new ExtendedSequenceNumber(sequenceNumber),
                parentShardId
        );
    }

    /**
     * Creates a MultiStreamLease with complete configuration for testing purposes.
     *
     * @param streamArn The ARN of the stream
     * @param shardId The ID of the shard
     * @param checkpoint The checkpoint sequence number
     * @param parentShardId The parent shard ID (can be null)
     * @return A fully configured MultiStreamLease object
     */
    public static MultiStreamLease createTestLease(
            String streamArn,
            String shardId,
            ExtendedSequenceNumber checkpoint,
            String parentShardId) {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey(MultiStreamLease.getLeaseKey(streamArn, shardId));
        lease.streamIdentifier(streamArn);
        lease.shardId(shardId);
        lease.checkpoint(checkpoint);
        lease.parentShardIds(parentShardId != null ?
                Collections.singletonList(parentShardId) :
                Collections.emptyList());
        return lease;
    }

    public static String shardId(Instant createTime) {
        return "shardId-" + String.format("%019d", createTime.toEpochMilli()) + "-40810d86";
    }

    public static Shard findShardById(List<Shard> shards, String shardId) {
        return shards.stream()
                .filter(s -> s.shardId().equals(shardId))
                .findFirst()
                .orElse(null);
    }

    public static software.amazon.awssdk.services.dynamodb.model.Shard findDynamoDBShardById(
            List<software.amazon.awssdk.services.dynamodb.model.Shard> shards,
            String shardId) {
        return shards.stream()
                .filter(s -> s.shardId().equals(shardId))
                .findFirst()
                .orElse(null);
    }

    public static AwsServiceException createThrottlingException() {
        return AwsServiceException.builder()
                .message(TEST_ERROR_MESSAGE)
                .statusCode(400)
                .requestId(TEST_REQUEST_ID)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(AmazonServiceExceptionTransformer.DYNAMODB_STREAMS_THROTTLING_EXCEPTION_ERROR_CODE)
                        .errorMessage(TEST_ERROR_MESSAGE)
                        .serviceName("DynamoDB")
                        .build())
                .build();
    }
}