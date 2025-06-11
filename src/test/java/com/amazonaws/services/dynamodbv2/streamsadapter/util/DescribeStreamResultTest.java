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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


class DescribeStreamResultTest {

    @Test
    void testDefaultConstructor() {
        // Act
        DescribeStreamResult result = new DescribeStreamResult();

        // Assert
        Assertions.assertNotNull(result.getShards());
        Assertions.assertTrue(result.getShards().isEmpty());
        Assertions.assertEquals("DISABLED", result.getStreamStatus());
    }

    @Test
    void testAddShards() {
        // Arrange
        DescribeStreamResult result = new DescribeStreamResult();
        Shard shard1 = Shard.builder()
                .shardId("shard-1")
                .build();
        Shard shard2 = Shard.builder()
                .shardId("shard-2")
                .build();
        List<Shard> shardList = Arrays.asList(shard1, shard2);

        // Act
        result.addShards(shardList);

        // Assert
        Assertions.assertEquals(2, result.getShards().size());
        Assertions.assertTrue(result.getShards().contains(shard1));
        Assertions.assertTrue(result.getShards().contains(shard2));
    }

    @Test
    void testAddMultipleShardLists() {
        // Arrange
        DescribeStreamResult result = new DescribeStreamResult();
        Shard shard1 = Shard.builder()
                .shardId("shard-1")
                .build();
        Shard shard2 = Shard.builder()
                .shardId("shard-2")
                .build();
        Shard shard3 = Shard.builder()
                .shardId("shard-3")
                .build();

        // Act
        result.addShards(Arrays.asList(shard1, shard2));
        result.addShards(Collections.singletonList(shard3));

        // Assert
        Assertions.assertEquals(3, result.getShards().size());
        Assertions.assertTrue(result.getShards().contains(shard1));
        Assertions.assertTrue(result.getShards().contains(shard2));
        Assertions.assertTrue(result.getShards().contains(shard3));
    }

    @Test
    void testSetStreamStatus() {
        // Arrange
        DescribeStreamResult result = new DescribeStreamResult();

        // Act
        result.setStreamStatus("DISABLED");

        // Assert
        Assertions.assertEquals("DISABLED", result.getStreamStatus());
    }

    @Test
    void testShardsListIsMutable() {
        // Arrange
        DescribeStreamResult result = new DescribeStreamResult();
        Shard shard = Shard.builder()
                .shardId("shard-1")
                .build();

        // Act
        result.addShards(Collections.singletonList(shard));

        // Assert
        Assertions.assertNotNull(result.getShards());
        Assertions.assertEquals(1, result.getShards().size());
    }

    @Test
    void testAddEmptyShardList() {
        // Arrange
        DescribeStreamResult result = new DescribeStreamResult();

        // Act
        result.addShards(new ArrayList<>());

        // Assert
        Assertions.assertTrue(result.getShards().isEmpty());
    }
}
