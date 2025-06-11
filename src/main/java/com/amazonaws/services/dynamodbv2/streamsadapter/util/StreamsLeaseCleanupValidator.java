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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.MIN_LEASE_RETENTION_DURATION_IN_HOURS;
import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.getShardCreationTime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.MultiStreamLease;
import java.time.Instant;
import java.util.Set;

@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public final class StreamsLeaseCleanupValidator {
    private static final Log LOG = LogFactory.getLog(StreamsLeaseCleanupValidator.class);

    private StreamsLeaseCleanupValidator() {}

    /**
     * Validates if a lease is candidate for cleanup in multi-stream mode.
     *
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds List of leases currently held by the worker.
     * @param isMultiStreamMode Whether running in multi-stream mode
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
     *         shard (we are evaluating for deletion).
     */
    public static boolean isCandidateForCleanup(Lease lease,
                                                Set<String> currentKinesisShardIds,
                                                boolean isMultiStreamMode) throws KinesisClientLibIOException {
        boolean isCandidateForCleanup = true;

        // Extract the correct shardId based on stream mode
        String shardId;
        if (isMultiStreamMode) {
            if (!(lease instanceof MultiStreamLease)) {
                throw new IllegalArgumentException("Expected MultiStreamLease but got " + lease.getClass().getName());
            }
            shardId = ((MultiStreamLease) lease).shardId();
        } else {
            shardId = lease.leaseKey();
        }

        // During child shard discovery, there might be leases which are found in child shard but describestream
        // hasn't yet returned them, dont mark a lease as candidate for cleanup if its a new lease
        if (Instant.now().isBefore(getShardCreationTime(shardId).plus(MIN_LEASE_RETENTION_DURATION_IN_HOURS))) {
            return false;
        }

        if (currentKinesisShardIds.contains(shardId)) {
            isCandidateForCleanup = false;
        } else {
            LOG.info(String.format("Found lease for non-existent shard: %s. Checking its parent shards", shardId));
            Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {
                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards.
                if (currentKinesisShardIds.contains(parentShardId)) {
                    String message = "Parent shard " + parentShardId + " exists but not the child shard " + shardId;
                    LOG.info(message);
                    throw new KinesisClientLibIOException(message);
                }
            }
        }

        return isCandidateForCleanup;
    }

    /**
     * Overloaded method that defaults to single-stream mode for backward compatibility.
     */
    public static boolean isCandidateForCleanup(Lease lease, Set<String> currentKinesisShardIds)
            throws KinesisClientLibIOException {
        return isCandidateForCleanup(lease, currentKinesisShardIds, false);
    }
}
