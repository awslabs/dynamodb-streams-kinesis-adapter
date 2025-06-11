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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamoDBStreamsMultiStreamTrackerTest {
    private static final String STREAM_ARN_1 = "arn:aws:dynamodb:us-west-2:123456789012:table/Table1/stream/2024-02-03T00:00:00.000";
    private static final String STREAM_ARN_2 = "arn:aws:dynamodb:us-west-2:123456789012:table/Table2/stream/2024-02-03T00:00:00.000";
    private static final InitialPositionInStreamExtended INITIAL_POSITION =
            InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON);
    private static final FormerStreamsLeasesDeletionStrategy DELETION_STRATEGY =
            new FormerStreamsLeasesDeletionStrategy.NoLeaseDeletionStrategy();

    private DynamoDBStreamsMultiStreamTracker tracker;
    private List<StreamConfig> streamConfigList;

    @BeforeEach
    void setup() {
        streamConfigList = Arrays.asList(
                new StreamConfig(StreamIdentifier.singleStreamInstance(STREAM_ARN_1), INITIAL_POSITION),
                new StreamConfig(StreamIdentifier.singleStreamInstance(STREAM_ARN_2), INITIAL_POSITION)
        );
        tracker = new DynamoDBStreamsMultiStreamTracker(streamConfigList, DELETION_STRATEGY);
    }

    @Test
    void testIsMultiStream() {
        assertTrue(tracker.isMultiStream());
    }

    @Test
    void testStreamConfigList() {
        List<StreamConfig> configs = tracker.streamConfigList();
        assertNotNull(configs);
        assertEquals(2, configs.size());
        assertEquals(STREAM_ARN_1, configs.get(0).streamIdentifier().serialize());
        assertEquals(STREAM_ARN_2, configs.get(1).streamIdentifier().serialize());
    }

    @Test
    void testStreamConfigsAreImmutable() {
        List<StreamConfig> configs = tracker.streamConfigList();
        assertThrows(UnsupportedOperationException.class, () ->
                configs.add(new StreamConfig(
                        StreamIdentifier.singleStreamInstance(STREAM_ARN_1),
                        INITIAL_POSITION
                ))
        );
    }

    @Test
    void testFormerStreamsLeasesDeletionStrategy() {
        assertEquals(DELETION_STRATEGY, tracker.formerStreamsLeasesDeletionStrategy());
    }

    @Test
    void testStreamConfigsWithDifferentInitialPositions() {
        List<StreamConfig> mixedConfigs = Arrays.asList(
                new StreamConfig(
                        StreamIdentifier.singleStreamInstance(STREAM_ARN_1),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)
                ),
                new StreamConfig(
                        StreamIdentifier.singleStreamInstance(STREAM_ARN_2),
                        InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.LATEST)
                )
        );

        tracker = new DynamoDBStreamsMultiStreamTracker(mixedConfigs, DELETION_STRATEGY);
        List<StreamConfig> configs = tracker.streamConfigList();

        assertEquals(InitialPositionInStream.TRIM_HORIZON,
                configs.get(0).initialPositionInStreamExtended().getInitialPositionInStream());
        assertEquals(InitialPositionInStream.LATEST,
                configs.get(1).initialPositionInStreamExtended().getInitialPositionInStream());
    }

    @Test
    void testDuplicateStreamConfigs() {
        List<StreamConfig> duplicateConfigs = Arrays.asList(
                new StreamConfig(StreamIdentifier.singleStreamInstance(STREAM_ARN_1), INITIAL_POSITION),
                new StreamConfig(StreamIdentifier.singleStreamInstance(STREAM_ARN_1), INITIAL_POSITION)
        );

        tracker = new DynamoDBStreamsMultiStreamTracker(duplicateConfigs, DELETION_STRATEGY);
        List<StreamConfig> configs = tracker.streamConfigList();

        assertEquals(2, configs.size(), "Duplicate streams should be allowed");
    }

    @Test
    void testStreamIdentifierFormat() {
        List<StreamConfig> configs = tracker.streamConfigList();
        for (StreamConfig config : configs) {
            String serialized = config.streamIdentifier().serialize();
            assertTrue(serialized.matches("^arn:aws:dynamodb:[\\w-]+:\\d{12}:table/[\\w-]+/stream/[\\w:.\\-]+$"),
                    "Stream identifier should match DynamoDB stream ARN format");
        }
    }
}