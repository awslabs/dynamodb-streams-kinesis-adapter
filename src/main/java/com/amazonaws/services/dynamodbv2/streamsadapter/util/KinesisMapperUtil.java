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

import com.amazonaws.services.dynamodbv2.streamsadapter.serialization.RecordObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.HashKeyRange;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.StreamDescription;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public final class KinesisMapperUtil {

    private KinesisMapperUtil() {}
    /**
     * Pattern for a DynamoDB stream ARN. The valid format is
     * {@code arn:aws:dynamodb:<region>:<accountId>:table/<tableName>/stream/<streamLabel>}
     */
    private static final Pattern DYNAMODB_STREAM_ARN_PATTERN = Pattern.compile(
            "arn:aws[^:]*:dynamodb:(?<region>[-a-z0-9]+):(?<accountId>[0-9]{12}):table/(?<tableName>[^/]+)"
                    + "/stream/(?<streamLabel>.+)");

    private static final String DELIMITER = "$";
    private static final String COLON_REPLACEMENT = "_";
    private static final ObjectMapper MAPPER = new RecordObjectMapper();

    private static final String SHARD_ID_SEPARATOR = "-";
    private static Set<Region> awsRegions = new HashSet<>(Region.regions());
    
    /**
     * Add ddblocal to support streams on DynamoDB Local.
     */
    static {
        awsRegions.add(Region.of("ddblocal"));
    }

    /**
     * All the shard-leases should stay retained for at least 6 hours in the lease table.
     * This duration ensures that we don't prematurely delete leases that might be needed
     * for stream position recovery.
     */
    public static final Duration MIN_LEASE_RETENTION_DURATION_IN_HOURS = Duration.ofHours(6);

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    /**
     * Converts a DynamoDB Streams Shard to a Kinesis Shard.
     * @return {@link Shard} kinesisShard
     */
    public static Shard convertDynamoDBShardToKinesisShard(software.amazon.awssdk.services.dynamodb.model.Shard
                                                                   dynamoDBShard) {
        return Shard.builder()
                .shardId(dynamoDBShard.shardId())
                .parentShardId(dynamoDBShard.parentShardId())
                .adjacentParentShardId(null)
                .sequenceNumberRange(SequenceNumberRange.builder()
                        .startingSequenceNumber(dynamoDBShard.sequenceNumberRange().startingSequenceNumber())
                        .endingSequenceNumber(dynamoDBShard.sequenceNumberRange().endingSequenceNumber())
                        .build()
                )
                .hashKeyRange(HashKeyRange.builder()
                        .startingHashKey(BigInteger.ZERO.toString())
                        .endingHashKey(BigInteger.ONE.toString())
                        .build()
                )
                .build();
    }

    /**
     * Converts the DynamoDB Streams GetShardIterator response to Kinesis GetShardIterator response.
     * @return {@link GetShardIteratorResponse} kinesisGetShardIteratorResponse
     */
    public static GetShardIteratorResponse convertDynamoDBGetShardIteratorResponseToKinesisGetShardIteratorResponse(
            software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse dynamoDBGetShardIteratorResponse) {
        return GetShardIteratorResponse.builder()
                .shardIterator(dynamoDBGetShardIteratorResponse.shardIterator())
                .build();
    }

    /**
     * Converts the DynamoDB Streams ListStreams response to Kinesis ListStreams response.
     * @return {@link GetShardIteratorResponse} kinesisGetShardIteratorResponse
     */
    public static ListStreamsResponse convertDynamoDBListStreamsResponseToKinesisListStreamsResponse(
            software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse listStreamsResponse) {
        return ListStreamsResponse.builder()
                .streamNames(listStreamsResponse.streams().stream().map(Stream::streamArn).collect(Collectors.toList()))
                .hasMoreStreams(listStreamsResponse.lastEvaluatedStreamArn() != null)
                .build();
    }

    /**
     * Converts the DynamoDB Streams DescribeStream response to Kinesis DescribeStream response.
     * @return {@link DescribeStreamResponse} kinesisDescribeStreamResponse
     */
    public static DescribeStreamResponse convertDynamoDBDescribeStreamResponseToKinesisDescribeStreamResponse(
            software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse describeStreamResponse) {
        return DescribeStreamResponse.builder()
                .streamDescription(
                        StreamDescription.builder()
                                .streamName(describeStreamResponse.streamDescription().streamArn())
                                .shards(
                                        describeStreamResponse.streamDescription()
                                                .shards()
                                                .stream()
                                                .map(KinesisMapperUtil::convertDynamoDBShardToKinesisShard)
                                                .collect(Collectors.toList()))
                                .streamStatus(describeStreamResponse.streamDescription().streamStatus().toString())
                                .hasMoreShards(describeStreamResponse.streamDescription()
                                        .lastEvaluatedShardId() != null)
                                .build()
                )
                .build();
    }

    /**
     * This method extracts the shard creation time from the ShardId.
     *
     * @param shardId
     * @return instant at which the shard was created
     */
    public static Instant getShardCreationTime(String shardId) {
        return Instant.ofEpochMilli(Long.parseLong(shardId.split(SHARD_ID_SEPARATOR)[1]));
    }

    /**
     * Validates if the given string is a valid DynamoDB Stream ARN.
     */
    public static boolean isValidDynamoDBStreamArn(String arn) {
        if (arn == null) {
            return false;
        }
        return DYNAMODB_STREAM_ARN_PATTERN.matcher(arn).matches();
    }

    /**
     * Creates a Kinesis-format StreamIdentifier from a DynamoDB Stream ARN.
     * Converts stream label from colon to underscore to avoid issues with colon in shardId
     * Format: region$accountId$tableName$underscore_separated_streamLabel for single streaming case
     * Format: accountId:region$accountId$tableName$underscore_separated_streamLabel:1 for multi-streaming case
     * @return {@link String} streamIdentifier
     */
    public static String createKinesisStreamIdentifierFromDynamoDBStreamsArn(String dynamoDbStreamArn,
                                                                             boolean isMultiStreamMode) {
        Matcher matcher = DYNAMODB_STREAM_ARN_PATTERN.matcher(dynamoDbStreamArn);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid DynamoDB stream ARN format: " + dynamoDbStreamArn);
        }
        String region = matcher.group("region");
        String accountId = matcher.group("accountId");
        String tableName = matcher.group("tableName");
        String streamLabel = matcher.group("streamLabel").replace(":", COLON_REPLACEMENT);
        // Create a unique stream name that can be used for lease keys
        String kinesisStreamName = String.join(DELIMITER, region, accountId, tableName, streamLabel);

        if (isMultiStreamMode) {
            return String.join(":", accountId, kinesisStreamName, "1");
        }
        return kinesisStreamName;
    }

    /**
     * Converts the stream name created by {@link #createKinesisStreamIdentifierFromDynamoDBStreamsArn
     * (String, boolean)}}.
     * to DynamoDB stream ARN
     * @return {@link String} dynamoDBStreamArn
     */
    public static String createDynamoDBStreamsArnFromKinesisStreamName(String streamName) {
        boolean isMultiStreamMode = streamName.contains(":");
        String streamNameToUse = isMultiStreamMode ? streamName.split(":")[1] : streamName;
        String[] parts = streamNameToUse.split(Pattern.quote(DELIMITER));
        String region = parts[0];
        String accountId = parts[1];
        String tableName = parts[2];
        String streamLabel = parts[3].replace(COLON_REPLACEMENT, ":");
        Region awsRegion = Region.of(region);

        if (!awsRegions.contains(awsRegion)) {
            throw new IllegalArgumentException("Invalid DynamoDB stream ARN format: " + streamNameToUse);
        }

        String arnPartition = awsRegion.metadata().partition().id();
        String dynamoDBStreamArn = String.format("arn:%s:dynamodb:%s:%s:table/%s/stream/%s",
                arnPartition, region, accountId, tableName, streamLabel);
        if (!isValidDynamoDBStreamArn(dynamoDBStreamArn)) {
            throw new IllegalArgumentException("Invalid DynamoDB stream ARN: " + dynamoDBStreamArn);
        }
        return dynamoDBStreamArn;
    }
}
