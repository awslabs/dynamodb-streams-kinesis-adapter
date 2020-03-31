/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.impl.Lease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseTaker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * DynamoDBStreams implementation of ILeaseTaker. Contains logic for taking and load-balancing(taking and evicting) leases.
 */
public final class StreamsLeaseTaker<T extends Lease> implements ILeaseTaker<T> {

    private static final Log LOG = LogFactory.getLog(StreamsLeaseTaker.class);

    private static final int SCAN_RETRIES = 1;
    private static final int TAKE_RETRIES = 3;


    private static final Callable<Long> SYSTEM_CLOCK_CALLABLE = System::nanoTime;
    private final ILeaseManager<T> leaseManager;
    private final String workerIdentifier;
    private final long leaseDurationNanos;
    private int maxLeasesForWorker = Integer.MAX_VALUE;
    private final Map<String, T> allLeases = new HashMap<>();
    private long lastScanTimeNanos = 0L;
    private static String SHARD_END = SentinelCheckpoint.SHARD_END.toString();

    public StreamsLeaseTaker(final ILeaseManager<T> leaseManager,
        final String workerIdentifier, final long leaseDurationMillis) {
        this.leaseManager = leaseManager;
        this.workerIdentifier = workerIdentifier;
        this.leaseDurationNanos = TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);
    }

    /**
     * Worker will not acquire more than the specified max number of leases even if there are more
     * shards that need to be processed. This can be used in scenarios where a worker is resource constrained or
     * to prevent lease thrashing when small number of workers pick up all leases for small amount of time during
     * deployment.
     * Note that setting a low value may cause data loss (e.g. if there aren't enough Workers to make progress on all
     * shards). When setting the value for this property, one must ensure enough workers are present to process
     * shards and should consider future resharding, child shards that may be blocked on parent shards, some workers
     * becoming unhealthy, etc.
     *
     * @param maxLeasesForWorker Max leases this Worker can handle at a time
     * @return LeaseTaker
     */
    public StreamsLeaseTaker<T> maxLeasesForWorker(final int maxLeasesForWorker) {
        if (maxLeasesForWorker <= 0) {
            throw new IllegalArgumentException("maxLeasesForWorker should be >= 1");
        }
        this.maxLeasesForWorker = maxLeasesForWorker;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public Map<String, T> takeLeases() throws DependencyException, InvalidStateException {
        long startTime = System.currentTimeMillis();
        refreshAllLeases(SYSTEM_CLOCK_CALLABLE);

        final Set<T> expiredLeases = getExpiredLeases();
        final Map<String, Integer> leaseCountsByHost = getUnfinishedLeaseCountsByHost(expiredLeases);
        final int numWorkers = leaseCountsByHost.size();
        final List<T> allUnfinishedLeases = getUnfinishedLeases(allLeases.values());
        final List<T> allFinishedLeases = getFinishedLeases(allLeases.values());
        final int myUnfinishedLeasesCount = getMyLeaseCount(allUnfinishedLeases, expiredLeases);
        final int myFinishedLeasesCount = getMyLeaseCount(allFinishedLeases, expiredLeases);
        final int targetUnfinishedLeasesCount = getTargetUnfinishedLeasesCount(allUnfinishedLeases.size(), numWorkers);
        final int targetFinishedLeasesCount = getTargetFinishedLeasesCount(allFinishedLeases.size(), numWorkers);

        final int remainingSlotsForUnfinishedLeases = targetUnfinishedLeasesCount - myUnfinishedLeasesCount;
        final int remainingSlotsForFinishedLeases = targetFinishedLeasesCount - myFinishedLeasesCount;
        final List<T> unfinishedExpiredLeases = getUnfinishedLeases(expiredLeases);
        final List<T> leasesToTake = getLeasesToTakeFromExpiredLeases(unfinishedExpiredLeases, remainingSlotsForUnfinishedLeases);
        leasesToTake.addAll(getLeasesToTakeFromExpiredLeases(getFinishedLeases(expiredLeases), remainingSlotsForFinishedLeases));
        leasesToTake.addAll(getLeasesToSteal(leaseCountsByHost, remainingSlotsForUnfinishedLeases - unfinishedExpiredLeases.size(), targetUnfinishedLeasesCount, allUnfinishedLeases));

        LOG.info(String.format("Worker %s saw %d total leases, %d expired leases, %d workers."
                + "Unfinished lease target: %d leases, I have %d unfinished leases. Finished leases target is %d and "
                + "I have %d finished leases. I will take %d leases in total.",
            workerIdentifier,
            allLeases.size(),
            expiredLeases.size(),
            numWorkers,
            targetUnfinishedLeasesCount,
            myUnfinishedLeasesCount,
            targetFinishedLeasesCount,
            myFinishedLeasesCount,
            leasesToTake.size()));

        // Take leases before eviction.
        Map<String, T> takenLeases = takeLeases(leasesToTake);
        // If I have more than my share of finished leases, evict
        if (remainingSlotsForFinishedLeases < 0) {
            evictLeases(getLeasesToEvict(Math.abs(remainingSlotsForFinishedLeases)));
        }
        LOG.info(String.format("TakeLeases took %d seconds.", (System.currentTimeMillis() - startTime) / 1000));
        return takenLeases;
    }

    /*
     * Refreshes allLeases for this taker.
     * @param timeProvider Callable that will supply the time
     */
    private void refreshAllLeases(final Callable<Long> timeProvider) throws
        InvalidStateException,
        DependencyException {

        ProvisionedThroughputException lastException = null;
        for (int i = 1; i <= SCAN_RETRIES; i++) {
            try {
                updateAllLeases(timeProvider);
                lastException = null;
            } catch (ProvisionedThroughputException e) {
                LOG.info(String.format("Worker %s could not find expired leases on try %d out of %d",
                    workerIdentifier,
                    i,
                    SCAN_RETRIES));
                lastException = e;
            }
        }

        if (lastException != null) {
            LOG.error("Worker " + workerIdentifier
                    + " could not scan leases table, aborting takeLeases. Exception caught by last retry:",
                lastException);
        }
    }

    /**
     * Scans all leases and update lastRenewalTime. Add new leases and delete old leases.
     *
     * @param timeProvider callable that supplies the current time
     * @throws ProvisionedThroughputException if listLeases fails due to lack of provisioned throughput
     * @throws InvalidStateException if the lease table does not exist
     * @throws DependencyException if listLeases fails in an unexpected way
     */
    private void updateAllLeases(Callable<Long> timeProvider)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {

        List<T> freshList = leaseManager.listLeases();
        try {
            lastScanTimeNanos = timeProvider.call();
        } catch (Exception e) {
            throw new DependencyException("Exception caught from timeProvider", e);
        }

        // This set will hold the lease keys not updated by the previous listLeases call.
        Set<String> notUpdated = new HashSet<String>(allLeases.keySet());

        // Iterate over all leases, finding ones to try to acquire that haven't changed since the last iteration
        for (T lease : freshList) {
            String leaseKey = lease.getLeaseKey();

            T oldLease = allLeases.get(leaseKey);
            allLeases.put(leaseKey, lease);
            notUpdated.remove(leaseKey);

            if (oldLease != null) {
                // If we've seen this lease before...
                if (oldLease.getLeaseCounter().equals(lease.getLeaseCounter())) {
                    // ...and the counter hasn't changed, propagate the lastRenewalNanos time from the old lease
                    lease.setLastCounterIncrementNanos(oldLease.getLastCounterIncrementNanos());
                } else {
                    // ...and the counter has changed, set lastRenewalNanos to the time of the scan.
                    lease.setLastCounterIncrementNanos(lastScanTimeNanos);
                }
            } else {
                if (lease.getLeaseOwner() == null) {
                    // if this new lease is unowned, it's never been renewed.
                    lease.setLastCounterIncrementNanos(0L);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Treating new lease with key " + leaseKey
                            + " as never renewed because it is new and unowned.");
                    }
                } else {
                    // if this new lease is owned, treat it as renewed as of the scan
                    lease.setLastCounterIncrementNanos(lastScanTimeNanos);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Treating new lease with key " + leaseKey
                            + " as recently renewed because it is new and owned.");
                    }
                }
            }
        }

        // Remove dead leases from allLeases
        for (String key : notUpdated) {
            allLeases.remove(key);
        }
    }

    /**
     * @return List of leases that were expired as of our last scan.
     */
    private Set<T> getExpiredLeases() {
        return allLeases.values().stream()
            .filter(lease -> lease.isExpired(leaseDurationNanos, lastScanTimeNanos))
            .collect(Collectors.toSet());
    }

    /**
     * Count leases by host. Always includes myself, but otherwise only includes hosts that are currently holding
     * leases.
     * @param expiredLeases list of leases that are currently expired
     * @return map of workerIdentifier to lease count
     */
    private Map<String, Integer> getUnfinishedLeaseCountsByHost(final Set<T> expiredLeases) {
        Map<String, Integer> leaseCounts = new HashMap<>();
        // Compute the number of leases per worker by looking through allLeases and ignoring leases that have expired.
        allLeases.values().stream()
            .filter(lease -> !expiredLeases.contains(lease) && lease.getLeaseOwner() != null)
            .filter(KinesisClientLease.class::isInstance)
            .map(KinesisClientLease.class::cast)
            .forEach(lease -> {
                // If lease is for unfinished shard, add 1 else 0.
                int numToAdd = 0;
                if (lease.getCheckpoint() != null &&
                    !SHARD_END.equals(lease.getCheckpoint().getSequenceNumber())) {
                    numToAdd = 1;
                }
                leaseCounts.merge(lease.getLeaseOwner(), numToAdd, Integer::sum);
            });


        // If I have no leases, I wasn't represented in leaseCounts. Let's fix that.
        leaseCounts.putIfAbsent(workerIdentifier, 0);
        return leaseCounts;
    }

    /*
     * Filters provided list of leases to return unfinished-shard leases.
     * @param leases List of leases to be filtered.
     */
    private List<T> getUnfinishedLeases(final Collection<T> leases) {
        return leases.stream()
            .filter(lease -> lease instanceof KinesisClientLease)
            .filter(lease -> ((KinesisClientLease)lease).getCheckpoint() != null)
            .filter(lease -> !SHARD_END.equals(((KinesisClientLease)lease).getCheckpoint()
                .getSequenceNumber()))
            .collect(Collectors.toList());
    }

    /*
     * Filters provided list of leases to return finished-shard leases.
     * @param leases List of leases to be filtered.
     */
    private List<T> getFinishedLeases(final Collection<T> leases) {
        return leases.stream()
            .filter(lease -> lease instanceof KinesisClientLease)
            .filter(lease -> ((KinesisClientLease)lease).getCheckpoint() != null)
            .filter(lease -> SHARD_END.equals(((KinesisClientLease)lease).getCheckpoint()
                .getSequenceNumber()))
            .collect(Collectors.toList());
    }

    /*
     * Returns the number of unfinishedLeases that every worker should should ideally hold
     * @param allUnfinishedLeasesCount Total number of unfinished shard leases across all workers
     * @param numWorkers Total number of workers.
     */
    private int getTargetUnfinishedLeasesCount(final int allUnfinishedLeasesCount, final int numWorkers) {
        // We want to ceil the division while calculating targets when the division isn't perfect.
        // If we don't, none of the workers will try to go for (anyTypeLeaseCount % numLeases) leases. eg. anyTypeLeaseCount = 14, numWorkers = 3. 14/3 = 4. If we use 4, there will be 2 leases which will go unclaimed.
        int leaseCount;
        if (numWorkers >= allUnfinishedLeasesCount) {
            leaseCount = 1;
        } else {
            leaseCount = allUnfinishedLeasesCount / numWorkers + (allUnfinishedLeasesCount % numWorkers == 0 ? 0 : 1);
        }

        int leaseSpillover = Math.max(0, leaseCount - maxLeasesForWorker);

        if (leaseSpillover > 0) {
            // Log warning
            LOG.warn(String.format("Worker %s : target is %d unfinished shard leases and maxLeasesForWorker is %d."
                    + " Resetting target to %d, lease spillover is %d. "
                    + " Note that some shards may not be processed if no other workers are able to pick them up resulting in a possible stall.",
                workerIdentifier,
                leaseCount,
                maxLeasesForWorker,
                maxLeasesForWorker,
                leaseSpillover));
            leaseCount = maxLeasesForWorker;
        }
        return leaseCount;
    }

    /*
     * Returns the number of finishedLeases that every worker should should ideally hold
     * @param allUnfinishedLeasesCount Total number of unfinished shard leases across all workers
     * @param numWorkers Total number of workers.
     */
    private int getTargetFinishedLeasesCount(int allFinishedLeasesCount, int numWorkers) {
        // We want to ceil the division while calculating targets when the division isn't perfect.
        // If we don't, none of the workers will try to go for (anyTypeLeaseCount % numLeases) leases. eg. anyTypeLeaseCount = 14, numWorkers = 3. 14/3 = 4. If we use 4, there will be 2 leases which will go unclaimed.
        int leaseCount;
        if (numWorkers >= allFinishedLeasesCount) {
            leaseCount = 1;
        } else {
            leaseCount = allFinishedLeasesCount / numWorkers + (allFinishedLeasesCount % numWorkers == 0 ? 0 : 1);
        }
        return leaseCount;
    }

    /*
     * Returns the number of leases owned by current worker that haven't expired yet.
     * @return The non-expired lease count for current worker.
     */
    private int getMyLeaseCount(final List<T> leases, final Set<T> expiredLeases) {
        return  Math.toIntExact(leases.stream()
            .filter(lease -> !expiredLeases.contains(lease))
            .filter(lease -> workerIdentifier.equals(lease.getLeaseOwner()))
            .count());
    }

    /**
     * Gets the set of leases I should try to take based on the state of the system.
     * Naming convetion: SHARD_END checkpointed leases - finishedLeases; unfinishedLeases otherwise.
     * The current implementation balances finished and unfinished shard leases separately. In a future increment,
     * we could explore not balancing the finished shard leases at all (at least when cleanupLeasesUponShardCompletion is true)
     * @param expiredLeases list of leases we determined to be expired
     * @return set of leases to take.
     */
    private List<T> getLeasesToTakeFromExpiredLeases(final List<T> expiredLeases, int remainingSlots) {
        if (remainingSlots <= 0) {
            return new ArrayList<>();
        }
        Collections.shuffle(expiredLeases);
        return expiredLeases.stream()
            .limit(remainingSlots)
            .collect(Collectors.toList());
    }

    /**
     * Choose leases to steal by randomly selecting one or more (up to max) from the most loaded worker.
     * Stealing rules:
     *
     * Steal up to maxLeasesToStealAtOneTime leases from the most loaded worker if
     * a) he has > target leases and I need >= 1 leases : steal min(leases needed, maxLeasesToStealAtOneTime)
     * b) he has == target leases and I need > 1 leases : steal 1
     *
     * @param leaseCountsForHosts map of workerIdentifier to lease count
     * @param needed # of leases needed to reach the target leases for the worker
     * @param target target # of leases per worker
     * @return Leases to steal, or empty list if we should not steal
     */
    private List<T> getLeasesToSteal(final Map<String, Integer> leaseCountsForHosts, final int needed, final int target, final List<T> activeLeases) {
        List<T> leasesToSteal = new ArrayList<>();

        if (needed <= 0) {
            return leasesToSteal;
        }

        // Mapping denoting number of leases added to leasesToSteal per worker.
        final Map<String, Integer> leasesAddedSoFar = new HashMap<>();

        // Mapping containing number of extra leases every worker other than me has over the target.
        final Map<String, Integer> extraLeasesWithHosts = leaseCountsForHosts.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(workerIdentifier))
            .filter(entry -> entry.getValue() > target)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue() - target));

        int numWorkersToStealFrom = extraLeasesWithHosts.size();
        if (numWorkersToStealFrom <= 0) {
            return leasesToSteal;
        }
        int leaseCountToStealPerWorker = needed / numWorkersToStealFrom + ((needed % numWorkersToStealFrom > 0) ? 1 : 0);
        // Shuffle so that every worker get doesn't contest for same lease.
        Collections.shuffle(activeLeases);

        for (T lease : activeLeases) {
            String leaseOwner = lease.getLeaseOwner();
            if (leaseOwner != null) {
                int extraLeasesWithCurrentOwner = extraLeasesWithHosts.getOrDefault(leaseOwner, 0);
                int addedSoFarFromCurrentOwner = leasesAddedSoFar.getOrDefault(leaseOwner, 0);
                // If current owner has extra leases that can be stolen and
                // I can steal more from the current owner

                if (extraLeasesWithCurrentOwner  > 0 &&
                    addedSoFarFromCurrentOwner < leaseCountToStealPerWorker) {
                    extraLeasesWithHosts.put(leaseOwner, extraLeasesWithCurrentOwner  - 1);
                    leasesAddedSoFar.put(leaseOwner, addedSoFarFromCurrentOwner + 1);
                    leasesToSteal.add(lease);
                }

                if(leasesToSteal.size() >= needed) {
                    break;
                }
            }
        }

        for (T leaseToSteal : leasesToSteal) {
            LOG.info(String.format(
                "Worker %s needs %d leases. It will steal lease %s from %s",
                workerIdentifier,
                needed,
                leaseToSteal.getLeaseKey(),
                leaseToSteal.getLeaseOwner()));
        }

        LOG.info(String.format("Worker %s will try to steal total %d leases", workerIdentifier, leasesToSteal.size()));
        return leasesToSteal;
    }

    /**
     * Internal implementation of takeLeases. Takes a callable that can provide the time to enable test cases without
     * Thread.sleep. Takes a callable instead of a raw time value because the time needs to be computed as-of
     * immediately after the scan.
     *
     *
     * @return map of lease key to taken lease
     *
     * @throws DependencyException Thrown when there is a DynamoDb exception.
     * @throws InvalidStateException Represents error state.
     */
    private Map<String, T> takeLeases(List<T> leasesToTake) throws DependencyException, InvalidStateException {
        // Key is leaseKey
        Map<String, T> takenLeases = new HashMap<>();
        Set<String> untakenLeaseKeys = new HashSet<>();

        for (T lease : leasesToTake) {
            String leaseKey = lease.getLeaseKey();

            for (int i = 0; i < TAKE_RETRIES; i++) {
                try {
                    if (leaseManager.takeLease(lease, workerIdentifier)) {
                        lease.setLastCounterIncrementNanos(System.nanoTime());
                        takenLeases.put(leaseKey, lease);
                    } else {
                        untakenLeaseKeys.add(leaseKey);
                    }
                    break;
                } catch (ProvisionedThroughputException e) {
                    LOG.info(String.format("Could not take lease with key %s for worker %s on try %d out of %d due to capacity",
                        leaseKey,
                        workerIdentifier,
                        i,
                        TAKE_RETRIES));
                }
            }
        }

        if (takenLeases.size() > 0) {
            LOG.info(String.format("Worker %s successfully took %d leases: %s",
                workerIdentifier,
                takenLeases.size(),
                stringJoin(takenLeases.keySet(), ", ")));
        }

        if (untakenLeaseKeys.size() > 0) {
            LOG.info(String.format("Worker %s failed to take %d leases: %s",
                workerIdentifier,
                untakenLeaseKeys.size(),
                stringJoin(untakenLeaseKeys, ", ")));
        }
        return takenLeases;
    }

    /**
     * Returns a list of leases that can be evicted.
     * @param numLeasesToEvict Number of leases to evict.
     */
    private List<T> getLeasesToEvict(int numLeasesToEvict) {
        List<T> leasesToEvict = new ArrayList<>();
        for (T lease : allLeases.values()) {
            if (numLeasesToEvict <= 0) return leasesToEvict;
            if (workerIdentifier.equals(lease.getLeaseOwner()) && lease instanceof KinesisClientLease) {
                KinesisClientLease kinesisClientLease = (KinesisClientLease) lease;
                if (kinesisClientLease.getCheckpoint() != null) {
                    String sequenceNumber = kinesisClientLease.getCheckpoint().getSequenceNumber();
                    if (SHARD_END.equals(sequenceNumber)) {
                        leasesToEvict.add(lease);
                        numLeasesToEvict--;
                    }
                }
            }
        }
        return leasesToEvict;
    }

    /**
     * Drops passed in leases for the worker.
     * @param leases Leases to evict.
     */
    private void evictLeases(List<T> leases) throws
        DependencyException,
        InvalidStateException {
        for(T lease : leases) {
            LOG.info(String.format("Worker %s : LeaseTaker will try to evict lease %s", workerIdentifier, lease.getLeaseKey()));
            try {
                leaseManager.evictLease(lease);
            } catch (ProvisionedThroughputException e) {
                LOG.info(String.format("Worker %s could not evict leases to take due to capacity", workerIdentifier));
                return;
            }
        }
        LOG.info(String.format("Worker %s : LeaseTaker evicted %d leases", workerIdentifier, leases.size()));
    }

    /** Package access for testing purposes.
     *
     * @param strings Collections of strings to be joined
     * @param delimiter Joining delimiter
     * @return Joined string.
     */
    static String stringJoin(final Collection<String> strings, final String delimiter) {
        StringBuilder builder = new StringBuilder();
        boolean needDelimiter = false;
        for (String string : strings) {
            if (needDelimiter) {
                builder.append(delimiter);
            }
            builder.append(string);
            needDelimiter = true;
        }

        return builder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWorkerIdentifier() {
        return workerIdentifier;
    }
}

