/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.leases.exceptions.DependencyException;
import com.amazonaws.services.kinesis.leases.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.leases.exceptions.ProvisionedThroughputException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;
import com.amazonaws.services.kinesis.leases.interfaces.ILeaseManager;

/**
 * Mock Lease Manager by randomly throwing Leasing Exceptions.
 * Copied as-is from Kinesis Client Library.
 *
 */
public class ExceptionThrowingLeaseManager implements ILeaseManager<KinesisClientLease> {
    private static final Log LOG = LogFactory.getLog(ExceptionThrowingLeaseManager.class);
    private static final Throwable EXCEPTION_MSG = new Throwable("Test Exception");

    // Use array below to control in what situations we want to throw exceptions.
    private int[] leaseManagerMethodCallingCount;

    /**
     * Methods which we support (simulate exceptions).
     */
    public enum ExceptionThrowingLeaseManagerMethods {
        CREATELEASETABLEIFNOTEXISTS(0),
        LEASETABLEEXISTS(1),
        WAITUNTILLEASETABLEEXISTS(2),
        LISTLEASES(3),
        CREATELEASEIFNOTEXISTS(4),
        GETLEASE(5),
        RENEWLEASE(6),
        TAKELEASE(7),
        EVICTLEASE(8),
        DELETELEASE(9),
        DELETEALL(10),
        UPDATELEASE(11),
        NONE(Integer.MIN_VALUE);

        private Integer index;

        ExceptionThrowingLeaseManagerMethods(Integer index) {
            this.index = index;
        }

        Integer getIndex() {
            return this.index;
        }
    }

    // Define which method should throw exception and when it should throw exception.
    private ExceptionThrowingLeaseManagerMethods methodThrowingException = ExceptionThrowingLeaseManagerMethods.NONE;
    private int timeThrowingException = Integer.MAX_VALUE;

    // The real local lease manager which would do the real implementations.
    private final ILeaseManager<KinesisClientLease> leaseManager;

    /**
     * Constructor accepts lease manager as only argument.
     *
     * @param leaseManager which will do the real implementations
     */
    public ExceptionThrowingLeaseManager(ILeaseManager<KinesisClientLease> leaseManager) {
        this.leaseManager = leaseManager;
        this.leaseManagerMethodCallingCount = new int[ExceptionThrowingLeaseManagerMethods.values().length];
    }

    /**
     * Set parameters used for throwing exception.
     *
     * @param method which would throw exception
     * @param throwingTime defines what time to throw exception
     */
    public void setLeaseLeaseManagerThrowingExceptionScenario(ExceptionThrowingLeaseManagerMethods method, int throwingTime) {
        this.methodThrowingException = method;
        this.timeThrowingException = throwingTime;
    }

    /**
     * Reset all parameters used for throwing exception.
     */
    public void clearLeaseManagerThrowingExceptionScenario() {
        Arrays.fill(leaseManagerMethodCallingCount, 0);
        this.methodThrowingException = ExceptionThrowingLeaseManagerMethods.NONE;
        this.timeThrowingException = Integer.MAX_VALUE;
    }

    // Throw exception when the conditions are satisfied :
    // 1). method equals to methodThrowingException
    // 2). method calling count equals to what we want
    private void throwExceptions(String methodName, ExceptionThrowingLeaseManagerMethods method)
        throws DependencyException {
        // Increase calling count for this method
        leaseManagerMethodCallingCount[method.getIndex()]++;
        if (method.equals(methodThrowingException)
            && (leaseManagerMethodCallingCount[method.getIndex()] == timeThrowingException)) {
            // Throw Dependency Exception if all conditions are satisfied.
            LOG.debug("Throwing DependencyException in " + methodName);
            throw new DependencyException(EXCEPTION_MSG);
        }
    }

    @Override
    public boolean createLeaseTableIfNotExists(Long readCapacity, Long writeCapacity)
        throws ProvisionedThroughputException, DependencyException {
        throwExceptions("createLeaseTableIfNotExists",
            ExceptionThrowingLeaseManagerMethods.CREATELEASETABLEIFNOTEXISTS);

        return leaseManager.createLeaseTableIfNotExists(readCapacity, writeCapacity);
    }

    @Override
    public boolean leaseTableExists() throws DependencyException {
        throwExceptions("leaseTableExists", ExceptionThrowingLeaseManagerMethods.LEASETABLEEXISTS);

        return leaseManager.leaseTableExists();
    }

    @Override
    public boolean waitUntilLeaseTableExists(long secondsBetweenPolls, long timeoutSeconds) throws DependencyException {
        throwExceptions("waitUntilLeaseTableExists", ExceptionThrowingLeaseManagerMethods.WAITUNTILLEASETABLEEXISTS);

        return leaseManager.waitUntilLeaseTableExists(secondsBetweenPolls, timeoutSeconds);
    }

    @Override
    public List<KinesisClientLease> listLeases()
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("listLeases", ExceptionThrowingLeaseManagerMethods.LISTLEASES);

        return leaseManager.listLeases();
    }

    @Override
    public boolean createLeaseIfNotExists(KinesisClientLease lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("createLeaseIfNotExists", ExceptionThrowingLeaseManagerMethods.CREATELEASEIFNOTEXISTS);

        return leaseManager.createLeaseIfNotExists(lease);
    }

    @Override
    public boolean renewLease(KinesisClientLease lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("renewLease", ExceptionThrowingLeaseManagerMethods.RENEWLEASE);

        return leaseManager.renewLease(lease);
    }

    @Override
    public boolean takeLease(KinesisClientLease lease, String owner)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("takeLease", ExceptionThrowingLeaseManagerMethods.TAKELEASE);

        return leaseManager.takeLease(lease, owner);
    }

    @Override
    public boolean evictLease(KinesisClientLease lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("evictLease", ExceptionThrowingLeaseManagerMethods.EVICTLEASE);

        return leaseManager.evictLease(lease);
    }

    @Override
    public void deleteLease(KinesisClientLease lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("deleteLease", ExceptionThrowingLeaseManagerMethods.DELETELEASE);

        leaseManager.deleteLease(lease);
    }

    @Override
    public boolean updateLease(KinesisClientLease lease)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("updateLease", ExceptionThrowingLeaseManagerMethods.UPDATELEASE);

        return leaseManager.updateLease(lease);
    }

    @Override
    public KinesisClientLease getLease(String shardId)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("getLease", ExceptionThrowingLeaseManagerMethods.GETLEASE);

        return leaseManager.getLease(shardId);
    }

    @Override
    public void deleteAll() throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        throwExceptions("deleteAll", ExceptionThrowingLeaseManagerMethods.DELETEALL);

        leaseManager.deleteAll();
    }

    @Override
    public boolean isLeaseTableEmpty() throws DependencyException,
        InvalidStateException, ProvisionedThroughputException {
        return false;
    }

}

