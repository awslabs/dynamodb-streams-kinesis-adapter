/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.leases;

import com.amazonaws.services.kinesis.clientlibrary.lib.checkpoint.SentinelCheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Map;

public class StreamsLeaseTakerTest {

    private StreamsLeaseTaker<KinesisClientLease> leaseTaker;
    private static String SHARD_END = SentinelCheckpoint.SHARD_END.toString();
    private static String TRIM_HORIZON = SentinelCheckpoint.TRIM_HORIZON.toString();
    final ArrayList<KinesisClientLease> leaseList = new ArrayList<>();

    @Mock
    ILeaseManager<KinesisClientLease> leaseManager;

    @Mock
    KinesisClientLease unfinishedShardExpiredLease1;

    @Mock
    KinesisClientLease unfinishedShardActiveLease1;

    @Mock
    KinesisClientLease finishedShardExpiredLease1;

    @Mock
    KinesisClientLease finishedShardActiveLease1;

    @Mock
    KinesisClientLease unfinishedShardExpiredLease2;

    @Mock
    KinesisClientLease unfinishedShardActiveLease2;

    @Mock
    KinesisClientLease finishedShardExpiredLease2;

    @Mock
    KinesisClientLease finishedShardActiveLease2;

    @Mock
    KinesisClientLease extraLease1;

    @Mock
    KinesisClientLease extraLease2;

    @Mock
    KinesisClientLease extraLease3;

    @Mock
    ExtendedSequenceNumber finishedCheckpoint;

    @Mock
    ExtendedSequenceNumber unFinishedCheckpoint;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        when(unFinishedCheckpoint.getSequenceNumber()).thenReturn(TRIM_HORIZON);
        when(finishedCheckpoint.getSequenceNumber()).thenReturn(SHARD_END);
        setupLeases();
        leaseTaker = new StreamsLeaseTaker<>(leaseManager, "worker-1", 10000);
    }

    @Test
    public void testMaxLeasesForWorker_setsMaxLeasesForWorker() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        leaseTaker.maxLeasesForWorker(1);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn(null);
        leaseList.add(unfinishedShardActiveLease1);
        leaseList.add(unfinishedShardExpiredLease1);
        leaseList.add(unfinishedShardExpiredLease2);
        leaseList.add(extraLease1);
        // worker doesn't take leases despite there being leases to take
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testMaxLeasesForWorker_throwsIllegalArgumentException() {
        try {
            leaseTaker.maxLeasesForWorker(-3);
            fail("Method maxLeasesForWorker didn't throw IllegalArgumentException");
        } catch(IllegalArgumentException exception) {
            assertEquals(exception.getMessage(), "maxLeasesForWorker should be >= 1");
        }
    }

    @Test
    public void testTakeLeases_isSynchronized() throws NoSuchMethodException {
        Method method = StreamsLeaseTaker.class.getDeclaredMethod("takeLeases");
        assertTrue(Modifier.isSynchronized(method.getModifiers()));
    }

    @Test(expected = DependencyException.class)
    public void testTakesLeases_throwsDependencyExceptionWhenRefreshingLeases() throws
        ProvisionedThroughputException,
        InvalidStateException, DependencyException {
            when(leaseManager.listLeases()).thenThrow(new DependencyException(new Throwable()));
            leaseTaker.takeLeases();
    }

    @Test(expected = InvalidStateException.class)
    public void testTakesLeases_throwsInvalidStateExceptionWhenRefreshingLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.listLeases()).thenThrow(new InvalidStateException(new Throwable()));
        leaseTaker.takeLeases();
    }

    @Test
    public void testTakeLeases_takesNoLeasesBalancedDistributionAndNoExpiredLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        leaseList.add(unfinishedShardActiveLease1);
        leaseList.add(unfinishedShardActiveLease2);
        leaseList.add(finishedShardActiveLease1);
        leaseList.add(finishedShardActiveLease2);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakeLeases_takesNoLeasesBalancedDistributionEvenWithExpiredLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        leaseList.add(unfinishedShardActiveLease1);
        leaseList.add(unfinishedShardExpiredLease1);
        leaseList.add(unfinishedShardActiveLease2);
        leaseList.add(unfinishedShardExpiredLease2);
        leaseList.add(finishedShardActiveLease1);
        leaseList.add(finishedShardExpiredLease1);
        leaseList.add(finishedShardActiveLease2);
        leaseList.add(finishedShardExpiredLease2);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakeLeases_takesNoLeasesIfLeasesTableIsEmpty() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.listLeases()).thenReturn(new ArrayList<>());
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakeLeases_takesNoLeasesExpiredNoneCanBeStolenAnd() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        leaseList.add(unfinishedShardActiveLease2);
        leaseList.add(finishedShardActiveLease1);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakeLeases_takesExpiredLeasesIfNeeded() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.takeLease(Mockito.any(KinesisClientLease.class), eq("worker-1"))).thenReturn(true);
        leaseList.add(finishedShardExpiredLease1);
        leaseList.add(unfinishedShardExpiredLease2);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        Map<String, KinesisClientLease> leasesTaken = leaseTaker.takeLeases();
        assertEquals(leasesTaken.size(), 2);
        assertEquals(leasesTaken.get("shard3"), finishedShardExpiredLease1);
        assertEquals(leasesTaken.get("shard5"), unfinishedShardExpiredLease2);
    }

    @Test
    public void testTakeLeases_stealsLeasesWhenExpiredLeasesAreAbsent() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.takeLease(Mockito.any(KinesisClientLease.class), eq("worker-1"))).thenReturn(true);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-2");
        leaseList.add(finishedShardActiveLease2);
        leaseList.add(unfinishedShardActiveLease2);
        leaseList.add(extraLease1);
        Map<String, KinesisClientLease> leasesTaken = leaseTaker.takeLeases();
        assertEquals(leasesTaken.size(), 1);
        assertThat(leasesTaken.keySet().iterator().next(), isOneOf("shard9", "shard6"));
    }

    @Test
    public void testTakeLeases_stealsOnlyRemainderWhenUnfinishedShardExpiredLeasesArePresent() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.takeLease(Mockito.any(KinesisClientLease.class), eq("worker-1"))).thenReturn(true);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-2");
        when(extraLease2.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease2.getLeaseOwner()).thenReturn("worker-2");
        leaseList.add(finishedShardActiveLease2);
        leaseList.add(unfinishedShardActiveLease2);
        leaseList.add(unfinishedShardExpiredLease1);
        leaseList.add(extraLease1);
        leaseList.add(extraLease2);
        Map<String, KinesisClientLease> leasesTaken = leaseTaker.takeLeases();
        assertEquals(leasesTaken.size(), 2);
        // One expired, one stolen
        assertEquals(leasesTaken.get("shard1"), unfinishedShardExpiredLease1);
        leasesTaken.remove("shard1");
        assertThat(leasesTaken.keySet().iterator().next(), isOneOf("shard9", "shard10", "shard6"));
    }

    @Test
    public void testTakeLeases_doesNotStealFinishedShardLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.takeLease(Mockito.any(KinesisClientLease.class), eq("worker-1"))).thenReturn(true);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-2");
        when(extraLease2.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease2.getLeaseOwner()).thenReturn("worker-2");
        leaseList.add(extraLease1);
        leaseList.add(extraLease2);
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakeLeases_doesntStealWhenWorkerHasNoLeasesAndEqualDistribution() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.takeLease(Mockito.any(KinesisClientLease.class), eq("worker-1"))).thenReturn(true);
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-3");
        when(extraLease2.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease2.getLeaseOwner()).thenReturn("worker-3");
        when(extraLease3.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(extraLease3.getLeaseOwner()).thenReturn("worker-2");
        leaseList.add(extraLease1);
        leaseList.add(extraLease2);
        leaseList.add(extraLease3);
        leaseList.add(unfinishedShardActiveLease2);
        assertEquals(leaseTaker.takeLeases().size(), 0);
    }

    @Test
    public void testTakesLeases_evictsFinishedShardLeasesIfExtra() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-1");
        when(extraLease2.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease2.getLeaseOwner()).thenReturn("worker-1");
        leaseList.add(finishedShardActiveLease1);
        leaseList.add(extraLease1);
        leaseList.add(extraLease2);
        leaseList.add(unfinishedShardActiveLease2);
        leaseTaker.takeLeases();
        verify(leaseManager, times(1)).evictLease(finishedShardActiveLease1);
    }

    @Test(expected = DependencyException.class)
    public void testTakesLeases_throwsDependencyExceptionWhenEvictingLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-1");
        when(leaseManager.evictLease(Mockito.any(KinesisClientLease.class))).thenThrow(new DependencyException(new Throwable()));
        leaseList.add(finishedShardActiveLease1);
        leaseList.add(extraLease1);
        leaseList.add(unfinishedShardActiveLease2);
        leaseTaker.takeLeases();
    }

    @Test(expected = InvalidStateException.class)
    public void testTakesLeases_throwsInvalidStateExceptionWhenEvictingLeases() throws
        DependencyException,
        ProvisionedThroughputException,
        InvalidStateException {
        when(leaseManager.listLeases()).thenReturn(leaseList);
        when(extraLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(extraLease1.getLeaseOwner()).thenReturn("worker-1");
        when(leaseManager.evictLease(Mockito.any(KinesisClientLease.class))).thenThrow(new InvalidStateException(new Throwable()));
        leaseList.add(finishedShardActiveLease1);
        leaseList.add(extraLease1);
        leaseList.add(unfinishedShardActiveLease2);
        leaseTaker.takeLeases();
    }

    private void setupLeases() {
        setupWorkerOneLeases();
        setupWorkerTwoLeases();
        setupExtraLeases();
    }

    private void setupWorkerOneLeases() {
        when(unfinishedShardExpiredLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(unfinishedShardExpiredLease1.getLeaseOwner()).thenReturn("worker-1");
        when(unfinishedShardExpiredLease1.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(true);
        when(unfinishedShardExpiredLease1.getLeaseKey()).thenReturn("shard1");
        when(unfinishedShardActiveLease1.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(unfinishedShardActiveLease1.getLeaseOwner()).thenReturn("worker-1");
        when(unfinishedShardActiveLease1.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);
        when(unfinishedShardActiveLease1.getLeaseKey()).thenReturn("shard2");
        when(finishedShardExpiredLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(finishedShardExpiredLease1.getLeaseOwner()).thenReturn("worker-1");
        when(finishedShardExpiredLease1.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(true);
        when(finishedShardExpiredLease1.getLeaseKey()).thenReturn("shard3");
        when(finishedShardActiveLease1.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(finishedShardActiveLease1.getLeaseOwner()).thenReturn("worker-1");
        when(finishedShardActiveLease1.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);
        when(finishedShardActiveLease1.getLeaseKey()).thenReturn("shard4");
    }

    private void setupWorkerTwoLeases() {
        when(unfinishedShardExpiredLease2.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(unfinishedShardExpiredLease2.getLeaseOwner()).thenReturn("worker-2");
        when(unfinishedShardExpiredLease2.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(true);
        when(unfinishedShardExpiredLease2.getLeaseKey()).thenReturn("shard5");
        when(unfinishedShardActiveLease2.getCheckpoint()).thenReturn(unFinishedCheckpoint);
        when(unfinishedShardActiveLease2.getLeaseOwner()).thenReturn("worker-2");
        when(unfinishedShardActiveLease2.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);
        when(unfinishedShardActiveLease2.getLeaseKey()).thenReturn("shard6");
        when(finishedShardExpiredLease2.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(finishedShardExpiredLease2.getLeaseOwner()).thenReturn("worker-2");
        when(finishedShardExpiredLease2.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(true);
        when(finishedShardExpiredLease2.getLeaseKey()).thenReturn("shard7");
        when(finishedShardActiveLease2.getCheckpoint()).thenReturn(finishedCheckpoint);
        when(finishedShardActiveLease2.getLeaseOwner()).thenReturn("worker-2");
        when(finishedShardActiveLease2.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);
        when(finishedShardActiveLease2.getLeaseKey()).thenReturn("shard8");
    }

    private void setupExtraLeases() {
        when(extraLease1.getLeaseKey()).thenReturn("shard9");
        when(extraLease1.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);

        when(extraLease2.getLeaseKey()).thenReturn("shard10");
        when(extraLease2.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);

        when(extraLease3.getLeaseKey()).thenReturn("shard11");
        when(extraLease3.isExpired(Mockito.anyLong(), Mockito.anyLong())).thenReturn(false);
    }
}

