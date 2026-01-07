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

import org.junit.jupiter.api.Test;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Assertions;

class StreamsLeaseCleanupValidatorTest {

    @Test
    void testIsCandidateForCleanup_SingleStream_LeaseExists() {
        Lease lease = new Lease();
        lease.leaseKey("shard-1");
        Set<String> currentShardIds = new HashSet<>(Arrays.asList("shard-1", "shard-2"));

        Assertions.assertFalse(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_MultiStream_LeaseExists() {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey("stream-1:shard-1");
        lease.shardId("shard-1");
        Set<String> currentShardIds = new HashSet<>(Arrays.asList("shard-1", "shard-2"));

        Assertions.assertFalse(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, true));
    }

    @Test
    void testIsCandidateForCleanup_SingleStream_LeaseDoesNotExist_NoParents() {
        Lease lease = new Lease();
        lease.leaseKey("shard-1");
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-2"));

        Assertions.assertTrue(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_MultiStream_LeaseDoesNotExist_NoParents() {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey("stream-1:shard-1");
        lease.shardId("shard-1");
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-2"));

        Assertions.assertTrue(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, true));
    }

    @Test
    void testIsCandidateForCleanup_SingleStream_LeaseDoesNotExist_ParentsDoNotExist() {
        Lease lease = new Lease();
        lease.leaseKey("shard-3");
        lease.parentShardIds(new HashSet<>(Arrays.asList("shard-1", "shard-2")));
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-4"));

        Assertions.assertTrue(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_MultiStream_LeaseDoesNotExist_ParentsDoNotExist() {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey("stream-1:shard-3");
        lease.shardId("shard-3");
        lease.parentShardIds(new HashSet<>(Arrays.asList("shard-1", "shard-2")));
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-4"));

        Assertions.assertTrue(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, true));
    }

    @Test
    void testIsCandidateForCleanup_SingleStream_LeaseDoesNotExist_ParentExists() {
        Lease lease = new Lease();
        lease.leaseKey("shard-3");
        lease.parentShardIds(new HashSet<>(Arrays.asList("shard-1", "shard-2")));
        Set<String> currentShardIds = new HashSet<>(Arrays.asList("shard-1", "shard-4"));

        Assertions.assertFalse(() ->
                StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_MultiStream_LeaseDoesNotExist_ParentExists() {
        MultiStreamLease lease = new MultiStreamLease();
        lease.leaseKey("stream-1:shard-3");
        lease.shardId("shard-3");
        lease.parentShardIds(new HashSet<>(Arrays.asList("shard-1", "shard-2")));
        Set<String> currentShardIds = new HashSet<>(Arrays.asList("shard-1", "shard-4"));

        Assertions.assertFalse(() ->
                StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, true));
    }

    @Test
    void testIsCandidateForCleanup_NullLease() {
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-1"));

        Assertions.assertThrows(NullPointerException.class, () ->
                StreamsLeaseCleanupValidator.isCandidateForCleanup(null, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_NullCurrentShardIds() {
        Lease lease = new Lease();
        lease.leaseKey("shard-1");

        Assertions.assertThrows(NullPointerException.class, () ->
                StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, null, false));
    }

    @Test
    void testIsCandidateForCleanup_EmptyCurrentShardIds() {
        Lease lease = new Lease();
        lease.leaseKey("shard-1");
        Set<String> currentShardIds = Collections.emptySet();

        Assertions.assertTrue(StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false));
    }

    @Test
    void testIsCandidateForCleanup_IllegalArgumentException_WhenWrongLeaseType() {
        Lease lease = new Lease(); // Using regular Lease in multi-stream mode
        lease.leaseKey("shard-1");
        Set<String> currentShardIds = new HashSet<>(Collections.singletonList("shard-2"));

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class, () ->
                StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, true));

        Assertions.assertEquals("Expected MultiStreamLease but got software.amazon.kinesis.leases.Lease", exception.getMessage());
    }

    // Test for backward compatibility
    @Test
    void testIsCandidateForCleanup_BackwardCompatibility() {
        Lease lease = new Lease();
        lease.leaseKey("shard-1");
        Set<String> currentShardIds = new HashSet<>(Arrays.asList("shard-1", "shard-2"));

        // Using the old method signature should be equivalent to using false for isMultiStreamMode
        boolean resultOld = StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds);
        boolean resultNew = StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentShardIds, false);

        Assertions.assertEquals(resultOld, resultNew);
    }
}