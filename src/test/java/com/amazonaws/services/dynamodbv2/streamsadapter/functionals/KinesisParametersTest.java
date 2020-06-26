/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.functionals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.BillingModeSummary;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.TestRecordProcessorFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtil;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

public class KinesisParametersTest extends FunctionalTestBase {
    private static final Log LOG = LogFactory.getLog(KinesisParametersTest.class);

    private static String KCL_WORKER_ID = "kcl-integration-test-worker";
    private static long IDLE_TIME_2S = 2000L;

    @Test
    public void leaseTableThroughputTest() throws Exception {
        KinesisClientLibConfiguration workerConfig =
            new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withInitialLeaseTableReadCapacity(50).withInitialLeaseTableWriteCapacity(50);

        startKCLWorker(workerConfig);

        while (((TestRecordProcessorFactory) recordProcessorFactory).getNumRecordsProcessed() < 0) {
            LOG.info("Sleep till RecordProcessor is initialized");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        DescribeTableResult describeTableResult = TestUtil.describeTable(dynamoDBClient, leaseTable);
        TableDescription leaseTableDescription = describeTableResult.getTable();
        ProvisionedThroughputDescription leaseTableThroughput = leaseTableDescription.getProvisionedThroughput();

        assertEquals(new Long(50), leaseTableThroughput.getReadCapacityUnits());
        assertEquals(new Long(50), leaseTableThroughput.getWriteCapacityUnits());
    }

    /**
     * This test configures KCL to call processRecords even when getRecords call returns nothing. The idle time setting
     * determines how many getRecords() calls will be made per second
     *
     * @throws Exception
     */
    @Test
    public void numProcessRecordsCallsTest() throws Exception {
        KinesisClientLibConfiguration workerConfig =
            new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID).withMaxRecords(10).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withCallProcessRecordsEvenForEmptyRecordList(true).withIdleTimeBetweenReadsInMillis(IDLE_TIME_2S);

        startKCLWorker(workerConfig);

        while (((TestRecordProcessorFactory) recordProcessorFactory).getNumRecordsProcessed() < 0) {
            LOG.info("Sleep till RecordProcessor is initialized");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        // Let KCL run for another 5 seconds
        Thread.sleep(THREAD_SLEEP_5S);

        shutDownKCLWorker();

        int numGetRecordsCalls = recordProcessorFactory.getNumProcessRecordsCalls();

        LOG.info("Num getRecords calls: " + numGetRecordsCalls);
        // Atleast 1 and atmost 2 getRecords/processRecords calls should have been made
        assertTrue(numGetRecordsCalls > 0 && numGetRecordsCalls <= 3);
    }

    /**
     * This test configures the worker with a non-default billing mode and ensures that the billing mode is passed
     * through to the created lease table.
     */
    @Test
    public void billingModeTest() throws Exception {
        KinesisClientLibConfiguration workerConfig =
                new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID)
                        .withBillingMode(BillingMode.PAY_PER_REQUEST);

        startKCLWorker(workerConfig);

        while (recordProcessorFactory.getNumRecordsProcessed() < 0) {
            LOG.info("Sleep till RecordProcessor is initialized");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        DescribeTableResult describeTableResult = TestUtil.describeTable(dynamoDBClient, leaseTable);
        TableDescription leaseTableDescription = describeTableResult.getTable();
        BillingModeSummary billingModeSummary = leaseTableDescription.getBillingModeSummary();
        assertEquals(BillingMode.PAY_PER_REQUEST.toString(), billingModeSummary.getBillingMode());
    }
}