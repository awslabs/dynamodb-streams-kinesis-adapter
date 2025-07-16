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

import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.MIN_LEASE_RETENTION_DURATION_IN_HOURS;
import static com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil.getShardCreationTime;
import static software.amazon.kinesis.common.HashKeyRangeForLease.fromHashKeyRange;
import static java.util.Objects.nonNull;

import com.amazonaws.services.dynamodbv2.streamsadapter.util.KinesisMapperUtil;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.StreamsLeaseCleanupValidator;
import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.MultiStreamLease;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import java.io.Serializable;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A shard syncer implementation specifically for DynamoDB Streams that extends the Kinesis hierarchical shard syncer.
 * This class handles synchronization of shard and lease states for DynamoDB streams in Lease Table
 * (Lease Management). It is responsible for ensuring that the leases table is up-to-date with the shards
 * in the stream.
 */
@SuppressWarnings("checkstyle:AvoidInlineConditionals")
@SuppressFBWarnings(value = "SE_BAD_FIELD")
public class DynamoDBStreamsShardSyncer extends HierarchicalShardSyncer {
    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsShardSyncer.class);

    private final boolean isMultiStreamMode;
    private final String streamIdentifier;
    private final boolean cleanupLeasesOfCompletedShards;

    private final String streamArn;

    private final DeletedStreamListProvider deletedStreamListProvider;

    private static final BiFunction<Lease, MultiStreamArgs, String> SHARD_ID_FROM_LEASE_DEDUCER =
            (lease, multiStreamArgs) ->
                    multiStreamArgs.isMultiStreamMode()
                            ? ((MultiStreamLease) lease).shardId()
                            : lease.leaseKey();

    /**
     * Constructs a DynamoDBStreamsShardSyncer that can operate in either single or multi-stream mode.
     *
     * @param isMultiStreamMode Whether the syncer should operate in multi-stream mode
     * @param streamIdentifier The identifier for the stream being processed
     */
    public DynamoDBStreamsShardSyncer(final boolean isMultiStreamMode, final String streamIdentifier,
                                      final boolean cleanupLeasesOfCompletedShards) {
        this(isMultiStreamMode, streamIdentifier, cleanupLeasesOfCompletedShards, null);
    }

    /**
     * Constructs a DynamoDBStreamsShardSyncer with support for deleted stream tracking.
     *
     * @param isMultiStreamMode Whether the syncer should operate in multi-stream mode.
     * @param streamIdentifier The identifier for the stream being processed.
     * @param deletedStreamListProvider Provider for tracking deleted streams.
     * @param cleanupLeasesOfCompletedShards Whether to cleanup the leases of finished shards.
     */
    public DynamoDBStreamsShardSyncer(final boolean isMultiStreamMode,
                                      final String streamIdentifier,
                                      final boolean cleanupLeasesOfCompletedShards,
                                      final DeletedStreamListProvider deletedStreamListProvider) {
        this.isMultiStreamMode = isMultiStreamMode;
        this.streamIdentifier = streamIdentifier;
        this.deletedStreamListProvider = deletedStreamListProvider;
        this.cleanupLeasesOfCompletedShards = cleanupLeasesOfCompletedShards;
        this.streamArn = KinesisMapperUtil.createDynamoDBStreamsArnFromKinesisStreamName(streamIdentifier);
    }

    /**
     * Checks for new shards and creates leases for them if they don't already exist.
     * This method will:
     * 1. Get the current stream shards
     * 2. Sync the lease table with the current stream shards
     * 3. Create leases for any new shards discovered
     *
     * @param shardDetector Used to get information about the stream shards
     * @param leaseRefresher Used to create and update leases in lease table
     * @param initialPosition The position where processing should start for newly discovered shards
     * @param scope Metrics scope for recording operations
     * @param ignoreUnexpectedChildShards Whether to ignore child shards that violate assumptions
     * @param isLeaseTableEmpty Whether the lease table is currently empty
     * @return true if the sync operation was successful
     * @throws DependencyException If dependent services are unavailable
     * @throws InvalidStateException If the lease table is in an invalid state
     * @throws ProvisionedThroughputException If DynamoDB provisioned throughput is exceeded
     * @throws KinesisClientLibIOException If there are IO issues communicating with Kinesis
     */
    @Override
    public synchronized boolean checkAndCreateLeaseForNewShards(@NonNull final ShardDetector shardDetector,
                                                                final LeaseRefresher leaseRefresher,
                                                                final InitialPositionInStreamExtended initialPosition,
                                                                final MetricsScope scope,
                                                                final boolean ignoreUnexpectedChildShards,
                                                                final boolean isLeaseTableEmpty)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException,
            KinesisClientLibIOException {
        syncShardLeases(shardDetector, leaseRefresher, initialPosition, scope, ignoreUnexpectedChildShards,
                isLeaseTableEmpty);
        return true;
    }

    private void syncShardLeases(@NonNull final ShardDetector shardDetector,
                                 final LeaseRefresher leaseRefresher,
                                 final InitialPositionInStreamExtended initialPosition,
                                 final MetricsScope scope,
                                 final boolean ignoreUnexpectedChildShards,
                                 final boolean isLeaseTableEmpty)
            throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        LOG.info("syncShardLeases " + streamArn  + ": begin");
        long startTimeMillis = System.currentTimeMillis();
        String consumerId = leaseRefresher.getLeaseTableIdentifier();
        List<Shard> shards = getShardList(shardDetector, consumerId);
        LOG.debug("Num shards " + streamArn  + ": " + shards.size());
        Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }

        List<Lease> currentLeases = isMultiStreamMode
                ? leaseRefresher.listLeasesForStream(shardDetector.streamIdentifier()) : leaseRefresher.listLeases();
        final MultiStreamArgs multiStreamArgs = new MultiStreamArgs(isMultiStreamMode,
                shardDetector.streamIdentifier());
        List<Lease> newLeasesToCreate = determineNewLeasesToCreate(shards, currentLeases, initialPosition,
                inconsistentShardIds, multiStreamArgs);
        LOG.debug("Num new leases to create " + streamArn  + ": " + newLeasesToCreate.size());
        for (Lease lease : newLeasesToCreate) {
            long leaseCreationStartTimeMillis = System.currentTimeMillis();
            boolean success = false;
            try {
                success = leaseRefresher.createLeaseIfNotExists(lease);
            } finally {
                if (isMultiStreamMode) {
                    MetricsUtil.addOperation(scope, MetricsUtil.STREAM_IDENTIFIER, streamArn);
                }
                MetricsUtil.addSuccessAndLatency(scope, "CreateLease", success, leaseCreationStartTimeMillis,
                        MetricsLevel.DETAILED);

            }
        }

        List<Lease> trackedLeases = new ArrayList<>();
        if (!currentLeases.isEmpty()) {
            trackedLeases.addAll(currentLeases);
        }
        trackedLeases.addAll(newLeasesToCreate);
        cleanupGarbageLeases(shards, trackedLeases, shardDetector, leaseRefresher, multiStreamArgs);
        if (cleanupLeasesOfCompletedShards) {
            cleanupLeasesOfFinishedShards(currentLeases, shardIdToShardMap, shardIdToChildShardIdsMap,
                    trackedLeases, leaseRefresher, multiStreamArgs);
        }
        MetricsUtil.addLatency(scope, "ShardSyncLatency:" + streamArn, startTimeMillis, MetricsLevel.SUMMARY);
        LOG.info("syncShardLeases: " + streamArn  + ": end");
    }

    private List<Shard> getShardList(@NonNull final ShardDetector shardDetector, String consumerId)
            throws KinesisClientLibIOException {
        // Fallback to existing behavior for backward compatibility
        List<Shard> shardList = Collections.emptyList();
        try {
            shardList = shardDetector.listShards(consumerId);
        } catch (ResourceNotFoundException e) {
            if (nonNull(this.deletedStreamListProvider) && isMultiStreamMode) {
                deletedStreamListProvider.add(StreamIdentifier.multiStreamInstance(streamIdentifier));
            }
        }

        if (Objects.isNull(shardList)) {
            throw new KinesisClientLibIOException(
                    String.format("Could not get shards for the stream: %s - will retry getting the shard list",
                            streamArn));
        }

        return shardList;
    }

    /** Helper method to detect a race condition between fetching the shards via paginated DescribeStream calls
     * and a reshard operation.
     * @param inconsistentShardIds
     */
    private void assertAllParentShardsAreClosed(Set<String> inconsistentShardIds) {
        if (!inconsistentShardIds.isEmpty()) {
            String ids = StringUtils.join(inconsistentShardIds, ' ');
            throw new KinesisClientLibIOException(String.format("%d open child shards (%s) are inconsistent. "
                            + "This can happen due to a race condition between describeStream and a reshard operation.",
                    inconsistentShardIds.size(), ids));
        }
    }

    /**
     * Helper method to create a new Lease POJO for a shard.
     * Note: Package level access only for testing purposes
     *
     * @param shard
     */
    private static Lease newKCLLease(final Shard shard) {
        Lease newLease = new Lease();
        newLease.leaseKey(shard.shardId());
        setupLeaseProperties(shard, newLease);
        return newLease;
    }

    private static Lease newKCLMultiStreamLease(final Shard shard, final StreamIdentifier streamIdentifier) {
        MultiStreamLease newLease = new MultiStreamLease();
        newLease.leaseKey(MultiStreamLease.getLeaseKey(streamIdentifier.serialize(), shard.shardId()));
        newLease.streamIdentifier(streamIdentifier.serialize());
        newLease.shardId(shard.shardId());
        setupLeaseProperties(shard, newLease);
        return newLease;
    }

    private static void setupLeaseProperties(final Shard shard, final Lease lease) {
        List<String> parentShardIds = new ArrayList<>(2);
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        lease.parentShardIds(parentShardIds);
        lease.ownerSwitchesSinceCheckpoint(0L);
        lease.hashKeyRange(fromHashKeyRange(shard.hashKeyRange()));
    }

    /**
     * Determines which new leases need to be created based on the current state of the stream and existing leases.
     * This method implements the following rules:
     *
     * 1. If there are no leases for a shard, create a lease if:
     *    - The shard is a descendant of an existing lease (checkpoint will be TRIM_HORIZON), or
     *    - The initial position is TRIM_HORIZON (checkpoint will be TRIM_HORIZON), or
     *    - The initial position is LATEST (checkpoint will be that position)
     *
     * 2. For each open (no ending sequence number) shard without a lease:
     *    - Determine if it's a descendant of any current shard
     *    - Create leases for its ancestors if they don't exist and are descendants
     *    - The checkpoint for new leases will be determined based on whether they are
     *      descendants and the initial position specified
     *
     * @param shards List of all shards in the stream from the DescribeStream call
     * @param currentLeases List of current leases from the lease table
     * @param initialPosition The initial position specified for processing (LATEST/TRIM_HORIZON)
     * @param inconsistentShardIds Set of shard IDs that are identified as inconsistent
     *                            (e.g., open child shards with open parents)
     * @param multiStreamArgs Container for multi-stream mode configuration and stream identifier
     *
     * @return List of new leases to create, sorted by the starting sequence number of their
     *         corresponding shards to ensure parent shards are processed before child shards
     *
     * Note: The returned list maintains the parent-child ordering of shards to ensure proper
     * processing order when the leases are created.
     */
    @VisibleForTesting
    List<Lease> determineNewLeasesToCreate(final List<Shard> shards,
                                           final List<Lease> currentLeases,
                                           final InitialPositionInStreamExtended initialPosition,
                                           final Set<String> inconsistentShardIds,
                                           final MultiStreamArgs multiStreamArgs) {
        LOG.info("determineNewLeasesToCreate " + streamArn +  ": begin");
        final String streamIdentifier = Optional.ofNullable(multiStreamArgs.streamIdentifier())
                .map(StreamIdentifier::serialize).orElse("");
        Set<String> shardIdsOfCurrentLeases = currentLeases.stream()
                .peek(lease -> LOG.debug("Existing lease, streamIdentifier: " + streamIdentifier + " lease: " + lease))
                .map(lease -> SHARD_ID_FROM_LEASE_DEDUCER.apply(lease, multiStreamArgs))
                .collect(Collectors.toSet());

        Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();
        Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);
        List<Shard> openShards = getOpenShards(shards);
        Map<String, Boolean> memoizationContext = new HashMap<>();

        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            String shardId = shard.shardId();
            LOG.debug("Evaluating leases for open shard " + shardId + " and its ancestors.");
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                LOG.debug("Lease for shardId " + shardId + " already exists. Not creating a lease");
            } else if (inconsistentShardIds.contains(shardId)) {
                LOG.info(String.format("shardId: %s for stream: %s is an inconsistent child." +
                        " Not creating a lease", shardId, streamArn));
            } else {
                LOG.debug("Need to create a lease for shardId " + shardId);
                Lease newLease = multiStreamArgs.isMultiStreamMode
                        ? newKCLMultiStreamLease(shard, multiStreamArgs.streamIdentifier) : newKCLLease(shard);
                boolean isDescendant =
                        checkIfDescendantAndAddNewLeasesForAncestors(shardId,
                                initialPosition,
                                shardIdsOfCurrentLeases,
                                shardIdToShardMapOfAllKinesisShards,
                                shardIdToNewLeaseMap,
                                memoizationContext,
                                multiStreamArgs);

                /**
                 * If the shard is a descendant checkpoint will always be TRIM_HORIZON
                 */
                if (isDescendant) {
                    newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                } else {
                    newLease.checkpoint(convertToCheckpoint(initialPosition));
                }
                LOG.debug("Set checkpoint of " + newLease.leaseKey() + " to " + newLease.checkpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

        List<Lease> newLeasesToCreate = new ArrayList<>(shardIdToNewLeaseMap.values());
        Comparator<? super Lease> startingSequenceNumberComparator =
                new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards,
                        multiStreamArgs);
        newLeasesToCreate.sort(startingSequenceNumberComparator);
        LOG.info("determineNewLeasesToCreate " + streamArn + ": done");
        return newLeasesToCreate;
    }

    /**
     * Note: Package level access for testing purposes only.
     * Check if this shard is a descendant of a shard that is (or will be) processed.
     * Create leases for the ancestors of this shard as required.
     * See javadoc of determineNewLeasesToCreate() for rules and example.
     *
     * @param shardId The shardId to check.
     * @param initialPosition One of LATEST or TRIM_HORIZON. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param shardIdsOfCurrentLeases The shardIds for the current leases.
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @param shardIdToLeaseMapOfNewShards Add lease POJOs corresponding to ancestors to this map.
     * @param memoizationContext Memoization of shards that have been evaluated as part of the evaluation
     * @return true if the shard is a descendant of any current shard (lease already exists)
     */

    // CHECKSTYLE:OFF CyclomaticComplexity
    boolean checkIfDescendantAndAddNewLeasesForAncestors(String shardId,
                                                         software.amazon.kinesis.common.InitialPositionInStreamExtended
                                                                 initialPosition,
                                                         Set<String> shardIdsOfCurrentLeases,
                                                         Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
                                                         Map<String, Lease> shardIdToLeaseMapOfNewShards,
                                                         Map<String, Boolean> memoizationContext,
                                                         final MultiStreamArgs multiStreamArgs) {

        Boolean previousValue = memoizationContext.get(shardId);
        if (previousValue != null) {
            return previousValue;
        }

        boolean isDescendant = false;
        Shard shard;
        Set<String> parentShardIds;
        Set<String> descendantParentShardIds = new HashSet<String>();

        if ((shardId != null) && (shardIdToShardMapOfAllKinesisShards.containsKey(shardId))) {
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                // This shard is a descendant of a current shard.
                isDescendant = true;
                // We don't need to add leases of its ancestors,
                // because we'd have done it when creating a lease for this shard.
            } else {
                shard = shardIdToShardMapOfAllKinesisShards.get(shardId);
                parentShardIds = getParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                for (String parentShardId : parentShardIds) {
                    // Check if the parent is a descendant, and include its ancestors.
                    if (checkIfDescendantAndAddNewLeasesForAncestors(parentShardId,
                            initialPosition,
                            shardIdsOfCurrentLeases,
                            shardIdToShardMapOfAllKinesisShards,
                            shardIdToLeaseMapOfNewShards,
                            memoizationContext,
                            multiStreamArgs)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        LOG.debug("Parent shard " + parentShardId + " is a descendant.");
                    } else {
                        LOG.debug("Parent shard " + parentShardId + " is NOT a descendant.");
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            LOG.debug("Need to create a lease for shardId " + parentShardId);
                            Lease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);
                            if (lease == null) {
                                lease = multiStreamArgs.isMultiStreamMode
                                        ? newKCLMultiStreamLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId),
                                                multiStreamArgs.streamIdentifier())
                                        : newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
                                shardIdToLeaseMapOfNewShards.put(parentShardId, lease);
                            }

                            if (descendantParentShardIds.contains(parentShardId)) {
                                lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                            } else {
                                lease.checkpoint(convertToCheckpoint(initialPosition));
                            }
                        }
                    }
                } else {
                    // This shard should be included, if the customer wants to process all records in the stream
                    if (initialPosition.getInitialPositionInStream().equals(software.amazon.kinesis.common
                            .InitialPositionInStream.TRIM_HORIZON)) {
                        isDescendant = true;
                    }
                }

            }
        }

        memoizationContext.put(shardId, isDescendant);
        return isDescendant;
    }

    private void cleanupGarbageLeases(List<Shard> shards,
                                      List<Lease> trackedLeases,
                                      ShardDetector shardDetector,
                                      LeaseRefresher leaseRefresher,
                                      MultiStreamArgs multiStreamArgs) throws ProvisionedThroughputException,
            InvalidStateException, DependencyException {

        LOG.info("cleanupGarbageLeases: " + streamArn + ": begin");
        Set<String> kinesisShards = new HashSet<>();
        for (Shard shard : shards) {
            kinesisShards.add(shard.shardId());
        }
        // Check if there are leases for non-existent shards
        List<Lease> garbageLeases = new ArrayList<>();
        for (Lease lease : trackedLeases) {
            if (StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, kinesisShards, isMultiStreamMode)) {
                garbageLeases.add(lease);
            }
        }

        if (!garbageLeases.isEmpty()) {
            LOG.info("Found " + garbageLeases.size()
                    + " candidate leases for cleanup. Refreshing list of"
                    + " dynamoDb shards to pick up recent/latest shards from stream "
                    + streamArn
            );

            Set<String> currentKinesisShardIds = new HashSet<>();
            for (Shard shard : shards) {
                currentKinesisShardIds.add(shard.shardId());
            }

            for (Lease lease : garbageLeases) {
                if (StreamsLeaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds,
                        isMultiStreamMode)) {
                    LOG.info("Deleting lease for shard " + SHARD_ID_FROM_LEASE_DEDUCER.apply(lease, multiStreamArgs)
                            + " as it is not present in stream. "
                            + streamArn
                    );

                    leaseRefresher.deleteLease(lease);
                }
            }
        }
        LOG.info("cleanupGarbageLeases " + streamArn + ": done");
    }

    /**
     * Private helper method.
     * Clean up leases for shards that meet the following criteria:
     * a/ the shard has been fully processed (checkpoint is set to SHARD_END)
     * b/ we've begun processing all the child shards: we have leases for all child shards and their checkpoint is not
     *      TRIM_HORIZON.
     *
     * @param currentLeases List of leases we evaluate for clean up
     * @param shardIdToShardMap Map of shardId->Shard (assumed to include all Kinesis shards)
     * @param shardIdToChildShardIdsMap Map of shardId->childShardIds (assumed to include all Kinesis shards)
     * @param trackedLeases List of all leases we are tracking.
     * @param leaseRefresher Lease manager (will be used to delete leases)
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    private synchronized void cleanupLeasesOfFinishedShards(Collection<Lease> currentLeases,
                                                            Map<String, Shard> shardIdToShardMap,
                                                            Map<String, Set<String>> shardIdToChildShardIdsMap,
                                                            List<Lease> trackedLeases,
                                                            LeaseRefresher leaseRefresher,
                                                            MultiStreamArgs multiStreamArgs)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        LOG.info("cleanupLeasesOfFinishedShards " + streamArn + ": begin");
        Set<String> shardIdsOfClosedShards = new HashSet<>();
        List<Lease> leasesOfClosedShards = new ArrayList<>();
        for (Lease lease : currentLeases) {
            if (lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
                shardIdsOfClosedShards.add(SHARD_ID_FROM_LEASE_DEDUCER.apply(lease, multiStreamArgs));
                leasesOfClosedShards.add(lease);
            }
        }

        if (!leasesOfClosedShards.isEmpty()) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap,
                    shardIdToChildShardIdsMap,
                    shardIdsOfClosedShards);
            Comparator<? super Lease> startingSequenceNumberComparator
                    = new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMap, multiStreamArgs);
            leasesOfClosedShards.sort(startingSequenceNumberComparator);
            Map<String, Lease> trackedLeaseMap = constructShardIdToKCLLeaseMap(trackedLeases, multiStreamArgs);

            for (Lease leaseOfClosedShard : leasesOfClosedShards) {
                String closedShardId = SHARD_ID_FROM_LEASE_DEDUCER.apply(leaseOfClosedShard, multiStreamArgs);
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(closedShardId);
                if ((closedShardId != null) && (childShardIds != null) && (!childShardIds.isEmpty())) {
                    cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseRefresher,
                            multiStreamArgs);
                }
            }
        }
        LOG.info("cleanupLeasesOfFinishedShards: " + streamArn + ": done");
    }

    /**
     * Delete lease for the closed shard. Rules for deletion are:
     * a/ the checkpoint for the closed shard is SHARD_END,
     * b/ there are leases for all the childShardIds and their checkpoint is also SHARD_END
     * c/ the shard has existed longer than the minimum lease retention duration (6 hours)
     *The lease retention duration ensures that we don't prematurely delete leases for shards
     *that might still be needed for stream position recovery
     * Note: This method has package level access solely for testing purposes.
     *
     * @param closedShardId Identifies the closed shard
     * @param childShardIds ShardIds of children of the closed shard
     * @param trackedLeases shardId->KinesisClientLease map with all leases we are tracking (should not be null)
     * @param leaseRefresher
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    synchronized void cleanupLeaseForClosedShard(String closedShardId,
                                                 Set<String> childShardIds,
                                                 Map<String, Lease> trackedLeases,
                                                 LeaseRefresher leaseRefresher,
                                                 MultiStreamArgs multiStreamArgs)
            throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease leaseForClosedShard = trackedLeases.get(closedShardId);
        List<Lease> childShardLeases = new ArrayList<>();

        for (String childShardId : childShardIds) {
            Lease childLease = trackedLeases.get(childShardId);
            if (childLease != null) {
                childShardLeases.add(childLease);
            }
        }

        if ((leaseForClosedShard != null)
                && (leaseForClosedShard.checkpoint().equals(ExtendedSequenceNumber.SHARD_END))
                && (childShardLeases.size() == childShardIds.size())) {
            boolean okayToDelete = true;
            for (Lease lease : childShardLeases) {
                if (!lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
                    okayToDelete = false; // if any child is still being processed, don't delete lease for parent
                    break;
                }
            }

            try {
                if (Instant.now().isBefore(getShardCreationTime(closedShardId)
                        .plus(MIN_LEASE_RETENTION_DURATION_IN_HOURS))) {
                    // if parent was created within lease retention period, don't delete lease for parent
                    okayToDelete = false;
                }
            } catch (RuntimeException e) {
                LOG.info("Could not extract creation time from ShardId [" + closedShardId + "] " + streamArn);
                LOG.debug(e);
            }

            if (okayToDelete) {
                LOG.info("Deleting lease for shard " + SHARD_ID_FROM_LEASE_DEDUCER.apply(leaseForClosedShard,
                        multiStreamArgs) + " as it is eligible for cleanup - its child shard is check-pointed at "
                        + "SHARD_END for the stream "
                        + streamArn
                );
                leaseRefresher.deleteLease(leaseForClosedShard);
            }
        }
    }

    /**
     * Helper method to create a shardId->KinesisClientLease map.
     * Note: This has package level access for testing purposes only.
     * @param trackedLeaseList
     * @return
     */
    Map<String, Lease> constructShardIdToKCLLeaseMap(List<Lease> trackedLeaseList, MultiStreamArgs multiStreamArgs) {
        Map<String, Lease> trackedLeasesMap = new HashMap<>();
        for (Lease lease : trackedLeaseList) {
            trackedLeasesMap.put(SHARD_ID_FROM_LEASE_DEDUCER.apply(lease, multiStreamArgs), lease);
        }
        return trackedLeasesMap;
    }



    /**
     * Note: this has package level access for testing purposes.
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed .
     */
    synchronized void assertClosedShardsAreCoveredOrAbsent(Map<String, Shard> shardIdToShardMap,
                                                           Map<String, Set<String>> shardIdToChildShardIdsMap,
                                                           Set<String> shardIdsOfClosedShards) throws
            KinesisClientLibIOException {
        String exceptionMessageSuffix = "This can happen if we constructed the list of shards "
                + " while a reshard operation was in progress.";

        for (String shardId : shardIdsOfClosedShards) {
            Shard shard = shardIdToShardMap.get(shardId);
            if (Instant.now().isBefore(getShardCreationTime(shardId).plus(MIN_LEASE_RETENTION_DURATION_IN_HOURS))) {
                LOG.info("Delaying deleting Shard " + shardId + " till lease retention duration is reached. "
                        + streamArn);
                continue; // if parent was created within lease retention period, don't delete lease for parent
            }
            if (shard == null) {
                LOG.info("Shard " + shardId + " is not present in stream " + streamArn + "anymore.");
                continue;
            }

            String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
            if (endingSequenceNumber == null) {
                throw new KinesisClientLibIOException("Shard " + shardId
                        + " is not closed. " + exceptionMessageSuffix);
            }

            Set<String> childShardIds = shardIdToChildShardIdsMap.get(shardId);
            if (childShardIds == null) {
                throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId
                        + " has no children." + exceptionMessageSuffix);
            }
        }
    }



    /**
     * Helper method to get parent shardIds of the current shard - includes the parent shardIds if:
     * a/ they are not null
     * b/ if they exist in the current shard map (i.e. haven't  expired)
     *
     * @param shard Will return parents of this shard
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @return Set of parentShardIds
     */
    Set<String> getParentShardIds(Shard shard, Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
        Set<String> parentShardIds = new HashSet<>(2);
        String parentShardId = shard.parentShardId();
        if ((parentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(parentShardId)) {
            parentShardIds.add(parentShardId);
        }
        return parentShardIds;
    }

    private static ExtendedSequenceNumber convertToCheckpoint(final InitialPositionInStreamExtended position) {
        ExtendedSequenceNumber checkpoint = null;

        if (position.getInitialPositionInStream().equals(InitialPositionInStream.TRIM_HORIZON)) {
            checkpoint = ExtendedSequenceNumber.TRIM_HORIZON;
        } else {
            checkpoint = ExtendedSequenceNumber.LATEST;
        }
        return checkpoint;
    }

    /**
     * Helper method to return all the open shards for a stream.
     * Note: Package level access only for testing purposes.
     *
     * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
     * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
     */
    List<Shard> getOpenShards(List<Shard> allShards) {
        List<Shard> openShards = new ArrayList<>();
        for (Shard shard : allShards) {
            String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
            if (endingSequenceNumber == null) {
                openShards.add(shard);
                LOG.debug("Found open shard: " + shard.shardId());
            }
        }
        return openShards;
    }

    /**
     * Helper method to construct the list of inconsistent shards, which are open shards with non-closed ancestor
     * parent(s).
     * @param shardIdToChildShardIdsMap
     * @param shardIdToShardMap
     * @return Set of inconsistent open shard ids for shards having open parents.
     */
    private Set<String> findInconsistentShardIds(Map<String, Set<String>> shardIdToChildShardIdsMap,
                                                 Map<String, Shard> shardIdToShardMap) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : shardIdToChildShardIdsMap.entrySet()) {
            String parentShardId = entry.getKey();
            Shard parentShard = shardIdToShardMap.get(parentShardId);
            if ((parentShardId == null) || (parentShard.sequenceNumberRange().endingSequenceNumber() == null)) {
                Set<String> childShardIdsMap = entry.getValue();
                result.addAll(childShardIdsMap);
            }
        }
        return result;
    }


    /**
     * Helper method to construct a shardId->Shard map for the specified list of shards.
     *
     * @param shards List of shards
     * @return ShardId->Shard map
     */
    Map<String, Shard> constructShardIdToShardMap(List<Shard> shards) {
        return shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
    }

    /**
     * Helper method to construct shardId->setOfChildShardIds map.
     * Note: This has package access for testing purposes only.
     * @param shardIdToShardMap
     * @return
     */
    Map<String, Set<String>> constructShardIdToChildShardIdsMap(
            Map<String, Shard> shardIdToShardMap) {
        Map<String, Set<String>> shardIdToChildShardIdsMap = new HashMap<>();
        for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
            String shardId = entry.getKey();
            Shard shard = entry.getValue();
            String parentShardId = shard.parentShardId();
            if ((parentShardId != null) && (shardIdToShardMap.containsKey(parentShardId))) {
                Set<String> childShardIds =
                        shardIdToChildShardIdsMap.computeIfAbsent(parentShardId, k -> new HashSet<>());
                childShardIds.add(shardId);
            }
        }
        return shardIdToChildShardIdsMap;
    }

    /** Helper class to compare leases based on starting sequence number of the corresponding shards.
     *
     */
    @RequiredArgsConstructor
    private static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<Lease>, Serializable {
        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;
        private final  MultiStreamArgs multiStreamArgs;

        /**
         * Compares two leases based on the starting sequence number of corresponding shards.
         * If shards are not found in the shardId->shard map supplied, we do a string comparison on the shardIds.
         * We assume that lease1 and lease2 are:
         *     a/ not null,
         *     b/ shards (if found) have non-null starting sequence numbers
         *
         * {@inheritDoc}
         */
        @Override
        public int compare(final Lease lease1, final Lease lease2) {
            int result = 0;
            final String shardId1 = SHARD_ID_FROM_LEASE_DEDUCER.apply(lease1, multiStreamArgs);
            final String shardId2 = SHARD_ID_FROM_LEASE_DEDUCER.apply(lease2, multiStreamArgs);
            final Shard shard1 = shardIdToShardMap.get(shardId1);
            final Shard shard2 = shardIdToShardMap.get(shardId2);

            // If we found shards for the two leases, use comparison of the starting sequence numbers
            if (shard1 != null && shard2 != null) {
                BigInteger sequenceNumber1 = new BigInteger(shard1.sequenceNumberRange().startingSequenceNumber());
                BigInteger sequenceNumber2 = new BigInteger(shard2.sequenceNumberRange().startingSequenceNumber());
                result = sequenceNumber1.compareTo(sequenceNumber2);
            }

            if (result == 0) {
                result = shardId1.compareTo(shardId2);
            }

            return result;
        }
    }
    @Data
    @Accessors(fluent = true)
    @VisibleForTesting
    static class MultiStreamArgs {
        private final Boolean isMultiStreamMode;
        private final StreamIdentifier streamIdentifier;
    }
}
