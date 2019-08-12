/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.streamsadapter.StreamsDeterministicShuffleShardSyncLeaderDecider.DETERMINISTIC_SHUFFLE_SEED;
import static com.amazonaws.services.dynamodbv2.streamsadapter.StreamsDeterministicShuffleShardSyncLeaderDecider.PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StreamsDeterministicShuffleShardSyncLeaderDeciderTest extends PeriodicShardSyncTestBase {

    private static final String WORKER_ID = "worker-id";

    private KinesisClientLibConfiguration config;
    private StreamsDeterministicShuffleShardSyncLeaderDecider leaderDecider;

    @Mock
    private ILeaseManager<KinesisClientLease> leaseManager;

    @Mock
    private ScheduledExecutorService scheduledExecutorService;

    private int numShardSyncWorkers;

    @Before
    public void setup() {
        numShardSyncWorkers = PERIODIC_SHARD_SYNC_MAX_WORKERS_DEFAULT;
        leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, leaseManager, scheduledExecutorService, numShardSyncWorkers);
        config = new KinesisClientLibConfiguration("Test", null, null, null);
    }

    @Test
    public void testLeaderElectionWithNullLeases() {
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if leaders is null", isLeader);
    }

    @Test
    public void testLeaderElectionWithEmptyLeases() throws Exception{
        when(leaseManager.listLeases()).thenReturn(new ArrayList<>());
        boolean isLeader = leaderDecider.isLeader(WORKER_ID);
        assertTrue("IsLeader should return true if no leases are returned", isLeader);
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrder() throws Exception {
        List<KinesisClientLease> leases = getLeases(5, false /* duplicateLeaseOwner */, true /* activeLeases */);
        when(leaseManager.listLeases()).thenReturn(leases);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        for (String leader : expectedLeaders) {
            assertTrue(leaderDecider.isLeader(leader));
        }
        for (KinesisClientLease lease : leases) {
            if (!expectedLeaders.contains(lease.getLeaseOwner())) {
                assertFalse(leaderDecider.isLeader(lease.getLeaseOwner()));
            }
        }
    }

    @Test
    public void testElectedLeadersAsPerExpectedShufflingOrderWhenUniqueWorkersLessThanMaxLeaders() {
        this.numShardSyncWorkers = 5; // More than number of unique lease owners
        leaderDecider = new StreamsDeterministicShuffleShardSyncLeaderDecider(config, leaseManager, scheduledExecutorService, numShardSyncWorkers);
        List<KinesisClientLease> leases = getLeases(3, false /* duplicateLeaseOwner */, true /* activeLeases */);
        Set<String> expectedLeaders = getExpectedLeaders(leases);
        // All lease owners should be present in expected leaders set, and they should all be leaders.
        for (KinesisClientLease lease : leases) {
            assertTrue(leaderDecider.isLeader(lease.getLeaseOwner()));
            assertTrue(expectedLeaders.contains(lease.getLeaseOwner()));
        }
    }

    private Set<String> getExpectedLeaders(List<KinesisClientLease> leases) {
        List<String> uniqueHosts = leases.stream().filter(lease -> lease.getLeaseOwner() != null)
            .map(KinesisClientLease::getLeaseOwner).distinct().sorted().collect(Collectors.toList());

        Collections.shuffle(uniqueHosts, new Random(DETERMINISTIC_SHUFFLE_SEED));
        int numWorkers = Math.min(uniqueHosts.size(), this.numShardSyncWorkers);
        return new HashSet<>(uniqueHosts.subList(0, numWorkers));
    }
}
