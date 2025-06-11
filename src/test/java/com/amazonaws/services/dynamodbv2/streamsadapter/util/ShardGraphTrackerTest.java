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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

class ShardGraphTrackerTest {

    private ShardGraphTracker shardGraphTracker;

    @BeforeEach
    void setUp() {
        shardGraphTracker = new ShardGraphTracker();
    }

    @Test
    void testCollectShards() {
        // Given
        List<Shard> shards = Arrays.asList(
                TestUtils.createShard("shard-1", null, "100", null),
                TestUtils.createShard("shard-2", null, "200", null)
        );

        // When
        shardGraphTracker.collectShards(shards);

        // Then
        List<Shard> collectedShards = shardGraphTracker.getShards();
        assertEquals(2, collectedShards.size());
        assertNotNull(TestUtils.findShardById(collectedShards, "shard-1"));
        assertNotNull(TestUtils.findShardById(collectedShards, "shard-2"));
    }

    @Test
    void testCloseOpenParents() {
        // Given
        List<Shard> shards = Arrays.asList(
                TestUtils.createShard("parent-1", null, "100", null),  // Open parent
                TestUtils.createShard("child-1", "parent-1", "200", null),  // Child of parent-1
                TestUtils.createShard("standalone", null, "300", "400")  // Closed shard
        );
        shardGraphTracker.collectShards(shards);

        // When
        shardGraphTracker.closeOpenParents();

        // Then
        List<Shard> processedShards = shardGraphTracker.getShards();
        Shard parentShard = TestUtils.findShardById(processedShards, "parent-1");
        assertNotNull(parentShard);
        assertEquals(String.valueOf(Long.MAX_VALUE),
                parentShard.sequenceNumberRange().endingSequenceNumber());
    }

    @Test
    void testMarkLeafShardsActive() {
        // Given
        List<Shard> shards = Arrays.asList(
                TestUtils.createShard("parent-1", null, "100", "200"),
                TestUtils.createShard("child-1", "parent-1", "200", "300"),
                TestUtils.createShard("leaf-1", null, "400", "500")  // Leaf shard
        );
        shardGraphTracker.collectShards(shards);

        // When
        shardGraphTracker.markLeafShardsActive();

        // Then
        List<Shard> processedShards = shardGraphTracker.getShards();
        Shard leafShard = TestUtils.findShardById(processedShards, "leaf-1");
        assertNotNull(leafShard);
        assertNull(leafShard.sequenceNumberRange().endingSequenceNumber());
    }

    @Test
    void testComplexScenario() {
        // Given
        List<Shard> shards = Arrays.asList(
                // Open parent with two children
                TestUtils.createShard("parent-1", null, "100", null),
                TestUtils.createShard("child-1", "parent-1", "200", null),
                TestUtils.createShard("child-2", "parent-1", "300", null),

                // Closed parent with child
                TestUtils.createShard("parent-2", null, "400", "500"),
                TestUtils.createShard("child-3", "parent-2", "500", null),

                // Standalone leaf
                TestUtils.createShard("leaf-1", null, "600", "700")
        );
        shardGraphTracker.collectShards(shards);

        // When
        shardGraphTracker.closeOpenParents();
        shardGraphTracker.markLeafShardsActive();

        // Then
        List<Shard> processedShards = shardGraphTracker.getShards();
        assertEquals(6, processedShards.size());

        // Verify parent-1 is closed
        Shard parent1 = TestUtils.findShardById(processedShards, "parent-1");
        assertNotNull(parent1);
        assertEquals(String.valueOf(Long.MAX_VALUE),
                parent1.sequenceNumberRange().endingSequenceNumber());

        // Verify leaf-1 is active
        Shard leaf1 = TestUtils.findShardById(processedShards, "leaf-1");
        assertNotNull(leaf1);
        assertNull(leaf1.sequenceNumberRange().endingSequenceNumber());
    }

    @Test
    void testEmptyShardsList() {
        // When
        shardGraphTracker.collectShards(null);
        shardGraphTracker.closeOpenParents();
        shardGraphTracker.markLeafShardsActive();

        // Then
        assertTrue(shardGraphTracker.getShards().isEmpty());
    }

    @Test
    void testMultipleParentChildRelationships() {
        // Given
        List<Shard> shards = Arrays.asList(
                TestUtils.createShard("parent-1", null, "100", null),
                TestUtils.createShard("parent-2", null, "200", null),
                TestUtils.createShard("child-1", "parent-1", "300", null),
                TestUtils.createShard("child-2", "parent-2", "400", null),
                TestUtils.createShard("grandchild", "child-1", "500", null)
        );
        shardGraphTracker.collectShards(shards);

        // When
        shardGraphTracker.closeOpenParents();

        // Then
        List<Shard> processedShards = shardGraphTracker.getShards();

        // Verify both parents are closed
        Shard parent1 = TestUtils.findShardById(processedShards, "parent-1");
        Shard parent2 = TestUtils.findShardById(processedShards, "parent-2");

        assertNotNull(parent1);
        assertNotNull(parent2);
        assertEquals(String.valueOf(Long.MAX_VALUE),
                parent1.sequenceNumberRange().endingSequenceNumber());
        assertEquals(String.valueOf(Long.MAX_VALUE),
                parent2.sequenceNumberRange().endingSequenceNumber());
    }
}