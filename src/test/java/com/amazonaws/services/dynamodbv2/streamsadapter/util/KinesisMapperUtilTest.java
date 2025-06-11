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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class KinesisMapperUtilTest {

    @Test
    void testConvertDynamoDBShardToKinesisShard() {
        // Arrange
        software.amazon.awssdk.services.dynamodb.model.Shard dynamoDBShard = software.amazon.awssdk.services.dynamodb.model.Shard.builder()
       .shardId("shardId-123")
       .parentShardId("parentShardId-123")
       .sequenceNumberRange(software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
      .startingSequenceNumber("100")
      .endingSequenceNumber("200")
      .build())
       .build();

        // Act
        Shard kinesisShard = KinesisMapperUtil.convertDynamoDBShardToKinesisShard(dynamoDBShard);

        // Assert
        assertEquals("shardId-123", kinesisShard.shardId());
        assertEquals("parentShardId-123", kinesisShard.parentShardId());
        assertNull(kinesisShard.adjacentParentShardId());
        assertEquals("100", kinesisShard.sequenceNumberRange().startingSequenceNumber());
        assertEquals("200", kinesisShard.sequenceNumberRange().endingSequenceNumber());
        assertEquals("0", kinesisShard.hashKeyRange().startingHashKey());
        assertEquals("1", kinesisShard.hashKeyRange().endingHashKey());
    }

    @Test
    void testConvertDynamoDBGetShardIteratorResponse() {
        // Arrange
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse dynamoDBResponse =
                software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse.builder()
                        .shardIterator("iterator-123")
                        .build();

        // Act
        GetShardIteratorResponse kinesisResponse =
                KinesisMapperUtil.convertDynamoDBGetShardIteratorResponseToKinesisGetShardIteratorResponse(dynamoDBResponse);

        // Assert
        Assertions.assertEquals("iterator-123", kinesisResponse.shardIterator());
    }

    @Test
    void testCreateKinesisStreamNameSingleStreamFormat() {
        // Test ARN with special characters in table name
        String arnWithSpecialChars = "arn:aws:dynamodb:us-west-2:123456789012:table/Test-Table_123/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(arnWithSpecialChars, false);
        Assertions.assertEquals("us-west-2$123456789012$Test-Table_123$2024-02-03T00_00_00.000", result);
    }

    @Test
    void testCreateKinesisStreamNameMultiStreamFormat() {
        // Test multi-stream format
        String arn = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(arn, true);
        Assertions.assertEquals("123456789012:us-west-2$123456789012$TestTable$2024-02-03T00_00_00.000:1", result);

        // Test multi-stream format with special characters
        String arnWithSpecialChars = "arn:aws:dynamodb:us-west-2:123456789012:table/Test-Table_123/stream/2024-02-03T00:00:00.000";
        String specialResult = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(arnWithSpecialChars, true);
        Assertions.assertEquals("123456789012:us-west-2$123456789012$Test-Table_123$2024-02-03T00_00_00.000:1", specialResult);
    }

    @Test
    void testCreateDynamoDBStreamsArnEdgeCases() {
        // Test with missing components
        String invalidFormat = "region$account";
        assertThrows(ArrayIndexOutOfBoundsException.class, () ->
                KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(invalidFormat));

        // Test with empty components
        String emptyComponents = "region$$$label";
        assertThrows(IllegalArgumentException.class, () ->
                KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(emptyComponents));
    }

    void testSingleStreamIdentifierConsistency() {
        // Test that stream identifiers remain consistent through multiple conversions
        String originalArn = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
        String streamIdentifier = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(originalArn, false);
        String reconstructedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(streamIdentifier);

        Assertions.assertEquals(originalArn, reconstructedArn);
    }

    @Test
    void testMultiStreamIdentifierConsistency() {
        // Test that stream identifiers remain consistent through multiple conversions
        String originalArn = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000";
        String streamIdentifier = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(originalArn, true);
        // Extract the middle part (accountId$tableName$streamLabel) from multi-stream format
        String middlePart = streamIdentifier.split(":")[1];
        String reconstructedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(middlePart);

        Assertions.assertEquals(originalArn, reconstructedArn);
    }

    @Test
    void testValidateArnFormat() {
        // Valid ARNs
        Assertions.assertTrue(KinesisMapperUtil.isValidDynamoDBStreamArn(
                "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024-02-03T00:00:00.000"));

        // Invalid ARNs
        assertFalse(KinesisMapperUtil.isValidDynamoDBStreamArn(null));
        assertFalse(KinesisMapperUtil.isValidDynamoDBStreamArn(""));
        assertFalse(KinesisMapperUtil.isValidDynamoDBStreamArn("arn:aws:dynamodb:region:account:table/name")); // missing stream
        assertFalse(KinesisMapperUtil.isValidDynamoDBStreamArn("arn:aws:kinesis:region:account:stream/name")); // wrong service
    }

    @Test
    void testTableNameWithSpecialCharacters() {
        // Test various special characters in table names
        String[] specialTableNames = {
                "Table-Name",
                "Table_Name",
                "Table.Name",
                "Table123",
                "123Table",
                "Table-123_Test.1"
        };

        for (String tableName : specialTableNames) {
            String arn = String.format("arn:aws:dynamodb:us-west-2:123456789012:table/%s/stream/2024-02-03T00:00:00.000",
                    tableName);
            String result = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(arn, false);
            String reconstructedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(result);
            Assertions.assertEquals(arn, reconstructedArn);
        }
    }

    @Test
    void testStreamLabelFormats() {
        // Test various stream label formats
        String[] streamLabels = {
                "2024-02-03T00:00:00.000",
                "2024-02-03T00:00:00.999",
                "1970-01-01T00:00:00.000",
                "9999-12-31T23:59:59.999"
        };

        for (String label : streamLabels) {
            String arn = String.format("arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/%s",
                    label);
            Assertions.assertTrue(KinesisMapperUtil.isValidDynamoDBStreamArn(arn));
        }
    }

    @Test
    void testRoundTripConversionWithMultiStream() {
        // Test that converting to multi-stream format and back works correctly
        String originalArn = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024:02:03T00:00:00.000";
        String multiStreamFormat = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(originalArn, true);
        String recreatedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(multiStreamFormat);
        Assertions.assertEquals(originalArn, recreatedArn);
    }

    @Test
    void testRoundTripConversionWithSingleStream() {
        // Test that converting to multi-stream format and back works correctly
        String originalArn = "arn:aws:dynamodb:us-west-2:123456789012:table/TestTable/stream/2024:02:03T00:00:00.000";
        String multiStreamFormat = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(originalArn, false);
        String recreatedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(multiStreamFormat);
        Assertions.assertEquals(originalArn, recreatedArn);
    }

    @Test
    void testCreateKinesisStreamIdentifierWithSpecialCharactersInTableName() {
        String streamArn = "arn:aws:dynamodb:us-west-2:123456789012:table/Test-Table_123.456/stream/2024-02-03T00:00:00.000";
        String result = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(streamArn, false);

        // Verify special characters are preserved
        Assertions.assertTrue(result.contains("Test-Table_123.456"));

        // Verify round trip conversion
        String reconstructedArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(result);
        Assertions.assertEquals(streamArn, reconstructedArn);
    }

    @Test
    void testCreateKinesisStreamIdentifierWithEmptyTableName() {
        String streamArn = "arn:aws:dynamodb:us-west-2:123456789012:table//stream/2024-02-03T00:00:00.000";

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(streamArn, false)
        );

        assertTrue(exception.getMessage().contains("Invalid DynamoDB stream ARN"));
    }

    @Test
    void testCreateDynamoDBStreamsArnWithInvalidFormat() {
        String invalidStreamName = "invalid$format$missing$components";

        assertThrows(
                IllegalArgumentException.class,
                () -> KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(invalidStreamName)
        );
    }

    @Test
    void testStreamIdentifierWithMaxLengthValues() {
        // Create ARN with maximum allowed lengths for components
        String region = "us-west-2";
        String accountId = "123456789012";
        String tableName = String.join("", Collections.nCopies(255, "a")); // Max DynamoDB table name length
        String timestamp = "2024-02-03T00:00:00.000";

        String streamArn = String.format("arn:aws:dynamodb:%s:%s:table/%s/stream/%s", region, accountId, tableName, timestamp);

        // Test both single and multi-stream modes
        String singleStreamId = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(streamArn, false);
        String multiStreamId = KinesisMapperUtil.createKinesisStreamIdentifierFromDynamoDBStreamsArn(streamArn, true);

        assertNotNull(singleStreamId);
        assertNotNull(multiStreamId);

        // Verify round-trip conversion works for both
        Assertions.assertEquals(streamArn, KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(singleStreamId));
        Assertions.assertTrue(multiStreamId.startsWith(accountId + ":"));
    }

    @Test
    void testConvertDynamoDBDescribeStreamResponseToKinesisDescribeStreamResponse() {
        String STREAM_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000";
        // Create test DynamoDB shards
        software.amazon.awssdk.services.dynamodb.model.Shard shard1 =
                software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                        .shardId("shard-1")
                        .parentShardId("parent-1")
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("100")
                                        .endingSequenceNumber("200")
                                        .build()
                        )
                        .build();

        software.amazon.awssdk.services.dynamodb.model.Shard shard2 =
                software.amazon.awssdk.services.dynamodb.model.Shard.builder()
                        .shardId("shard-2")
                        .parentShardId("parent-2")
                        .sequenceNumberRange(
                                software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange.builder()
                                        .startingSequenceNumber("201")
                                        .endingSequenceNumber("300")
                                        .build()
                        )
                        .build();

        // Create DynamoDB DescribeStreamResponse
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse dynamoResponse =
                software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse.builder()
                        .streamDescription(
                                software.amazon.awssdk.services.dynamodb.model.StreamDescription.builder()
                                        .streamArn(STREAM_ARN)
                                        .streamStatus(software.amazon.awssdk.services.dynamodb.model.StreamStatus.ENABLED)
                                        .shards(Arrays.asList(shard1, shard2))
                                        .lastEvaluatedShardId("shard-2")
                                        .build()
                        )
                        .build();

        // Convert to Kinesis response
        software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse kinesisResponse =
                KinesisMapperUtil.convertDynamoDBDescribeStreamResponseToKinesisDescribeStreamResponse(dynamoResponse);

        // Verify conversion
        assertNotNull(kinesisResponse);
        assertNotNull(kinesisResponse.streamDescription());
        Assertions.assertEquals(STREAM_ARN, kinesisResponse.streamDescription().streamName());
        Assertions.assertEquals("ENABLED", kinesisResponse.streamDescription().streamStatusAsString());
        Assertions.assertTrue(kinesisResponse.streamDescription().hasMoreShards());

        // Verify shards
        List<Shard> kinesisShards = kinesisResponse.streamDescription().shards();
        Assertions.assertEquals(2, kinesisShards.size());

        // Verify first shard
        software.amazon.awssdk.services.kinesis.model.Shard kinesisShard1 = kinesisShards.get(0);
        Assertions.assertEquals("shard-1", kinesisShard1.shardId());
        Assertions.assertEquals("parent-1", kinesisShard1.parentShardId());
        Assertions.assertEquals("100", kinesisShard1.sequenceNumberRange().startingSequenceNumber());
        Assertions.assertEquals("200", kinesisShard1.sequenceNumberRange().endingSequenceNumber());

        // Verify second shard
        software.amazon.awssdk.services.kinesis.model.Shard kinesisShard2 = kinesisShards.get(1);
        Assertions.assertEquals("shard-2", kinesisShard2.shardId());
        Assertions.assertEquals("parent-2", kinesisShard2.parentShardId());
        Assertions.assertEquals("201", kinesisShard2.sequenceNumberRange().startingSequenceNumber());
        Assertions.assertEquals("300", kinesisShard2.sequenceNumberRange().endingSequenceNumber());
    }

    @Test
    void testConvertDynamoDBListStreamsResponseToKinesisListStreamsResponse() {
        // Create test stream ARNs
        String streamArn1 = "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000";
        String streamArn2 = "arn:aws:dynamodb:us-west-2:123456789012:table/Table2/stream/2024-02-03T00:00:00.000";

        // Create DynamoDB streams
        software.amazon.awssdk.services.dynamodb.model.Stream stream1 =
                software.amazon.awssdk.services.dynamodb.model.Stream.builder()
                        .streamArn(streamArn1)
                        .tableName("Table1")
                        .streamLabel("2024-02-03T00:00:00.000")
                        .build();

        software.amazon.awssdk.services.dynamodb.model.Stream stream2 =
                software.amazon.awssdk.services.dynamodb.model.Stream.builder()
                        .streamArn(streamArn2)
                        .tableName("Table2")
                        .streamLabel("2024-02-03T00:00:00.000")
                        .build();

        // Create DynamoDB ListStreamsResponse
        software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse dynamoResponse =
                software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse.builder()
                        .streams(Arrays.asList(stream1, stream2))
                        .lastEvaluatedStreamArn(streamArn2)
                        .build();

        // Convert to Kinesis response
        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse kinesisResponse =
                KinesisMapperUtil.convertDynamoDBListStreamsResponseToKinesisListStreamsResponse(dynamoResponse);

        // Verify conversion
        assertNotNull(kinesisResponse);
        Assertions.assertEquals(2, kinesisResponse.streamNames().size());
        Assertions.assertTrue(kinesisResponse.streamNames().contains(streamArn1));
        Assertions.assertTrue(kinesisResponse.streamNames().contains(streamArn2));
        Assertions.assertTrue(kinesisResponse.hasMoreStreams());
    }

    @Test
    void testConvertDynamoDBListStreamsResponseWithEmptyStreams() {
        software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse dynamoResponse =
                software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse.builder()
                        .streams(Collections.emptyList())
                        .build();

        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse kinesisResponse =
                KinesisMapperUtil.convertDynamoDBListStreamsResponseToKinesisListStreamsResponse(dynamoResponse);

        assertNotNull(kinesisResponse);
        assertTrue(kinesisResponse.streamNames().isEmpty());
        assertFalse(kinesisResponse.hasMoreStreams());
    }

    @Test
    void testConvertDynamoDBGetShardIteratorResponseWithNullIterator() {
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse dynamoResponse =
                software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse.builder()
                        .shardIterator(null)
                        .build();

        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse kinesisResponse =
                KinesisMapperUtil.convertDynamoDBGetShardIteratorResponseToKinesisGetShardIteratorResponse(dynamoResponse);

        assertNotNull(kinesisResponse);
        Assertions.assertNull(kinesisResponse.shardIterator());
    }
}
