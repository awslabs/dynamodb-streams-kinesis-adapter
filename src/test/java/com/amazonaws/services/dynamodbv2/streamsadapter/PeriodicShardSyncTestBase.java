/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;

public class PeriodicShardSyncTestBase {

    private static final String LEASE_KEY = "lease_key";
    private static final String LEASE_OWNER = "lease_owner";

    protected List<KinesisClientLease> getLeases(int count, boolean duplicateLeaseOwner, boolean activeLeases) {
        List<KinesisClientLease> leases = new ArrayList<>();
        for (int i=0;i<count;i++) {
            KinesisClientLease lease = new KinesisClientLease();
            lease.setLeaseKey(LEASE_KEY + i);
            lease.setCheckpoint(activeLeases ? ExtendedSequenceNumber.LATEST : ExtendedSequenceNumber.SHARD_END);
            lease.setLeaseCounter(new Random().nextLong());
            lease.setLeaseOwner(LEASE_OWNER + (duplicateLeaseOwner ? "" : i));
            leases.add(lease);
        }
        return leases;
    }
}

