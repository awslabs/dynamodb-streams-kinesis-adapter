/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.functionals;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.ReplicatingRecordProcessor;
import com.amazonaws.services.dynamodbv2.streamsadapter.util.TestUtil;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * This test runs KCL with the DynamoDB Streams Kinesis Adapter using a single partition table and no shard lineage in
 * an embedded DynamoDB local instance. A series of operations are performed on the source table and then replicated on
 * a destination table using the records received by the IRecordProcessor implementation. Finally, a scan/query is
 * performed on both tables to assert that the expected records are replicated.
 */
public class CorrectnessTest extends FunctionalTestBase {
    private static final Log LOG = LogFactory.getLog(CorrectnessTest.class);

    private int numItemsInSrcTable = 0;

    private static int NUM_INITIAL_ITEMS = 2;

    @Before
    public void setup() {
        super.setup();
        insertAndUpdateItems(NUM_INITIAL_ITEMS);
    }

    @Test
    public void trimHorizonTest() throws Exception {
        LOG.info("Starting single shard KCL integration test with TRIM_HORIZON.");

        KinesisClientLibConfiguration workerConfig =
            new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        startKCLWorker(workerConfig);

        while (recordProcessorFactory.getNumRecordsProcessed() < (2 * numItemsInSrcTable) /* Num of expected stream records */) {
            LOG.info("Sleep till all records are processed");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        ScanResult srcTableScan = TestUtil.scanTable(dynamoDBClient, srcTable);
        ScanResult destTableScan = TestUtil.scanTable(dynamoDBClient, destTable);
        assertEquals(srcTableScan.getItems(), destTableScan.getItems());
    }

    @Test
    public void latestTest() throws Exception {
        LOG.info("Starting single shard KCL integration test with LATEST.");

        KinesisClientLibConfiguration workerConfig =
            new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID).withInitialPositionInStream(InitialPositionInStream.LATEST);

        startKCLWorker(workerConfig);

        while (recordProcessorFactory.getNumRecordsProcessed() < 0) {
            LOG.info("Sleep till RecordProcessor is initialized");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        /* Only the following records will be processed by KCL since it is reading only the latest stream entries */
        int numNewItemsToInsert = 1;
        insertAndUpdateItems(numNewItemsToInsert);

        while (recordProcessorFactory.getNumRecordsProcessed() < 2 * numNewItemsToInsert) {
            LOG.info("Sleep till all records are processed");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        String lastInsertedPartitionKey = Integer.toString(100 + this.numItemsInSrcTable);
        QueryResult srcTableQuery = TestUtil.queryTable(dynamoDBClient, srcTable, lastInsertedPartitionKey);
        ScanResult destTableScan = TestUtil.scanTable(dynamoDBClient, destTable);
        assertEquals(srcTableQuery.getItems(), destTableScan.getItems());
    }

    /**
     * This test spawns a thread to periodically write items to the source table. It shuts down and restarts the KCL
     * worker while writes are happening (to simulate the real-world situation of a worker dying and another taking its
     * place). There are two things being verified here:
     * 1. New KCL worker resumes from the checkpoint
     * 2. All stream records are processed
     *
     * @throws Exception
     */
    @Test
    public void workerFailureTest() throws Exception {
        LOG.info("Starting single shard KCL worker failure test.");

        KinesisClientLibConfiguration workerConfig =
            new KinesisClientLibConfiguration(leaseTable, streamId, credentials, KCL_WORKER_ID).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        startKCLWorker(workerConfig);

        // A thread that keeps writing to the table every 2 seconds
        ScheduledExecutorService loadGeneratorService = Executors.newSingleThreadScheduledExecutor();
        loadGeneratorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                insertAndUpdateItems(1);
            }
        }, 0/* initialDelay */, 2/* period */, TimeUnit.SECONDS);

        while (recordProcessorFactory.getNumRecordsProcessed() < 10) {
            LOG.info("Sleep till first few records are processed");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        // Calculate number of records processed by first worker and also the number of processed-but-not-checkpointed
        // records, since checkpoint happens after every batch of 10 records
        int numRecordsProcessedByFirstWorker = recordProcessorFactory.getNumRecordsProcessed();
        int numRecordsNotCheckpointed = numRecordsProcessedByFirstWorker % ReplicatingRecordProcessor.CHECKPOINT_BATCH_SIZE;

        // Start a new worker
        startKCLWorker(workerConfig);

        while (recordProcessorFactory.getNumRecordsProcessed() < 0) {
            LOG.info("Sleep till RecordProcessor is initialized");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        loadGeneratorService.shutdown();

        if (!loadGeneratorService.awaitTermination(THREAD_SLEEP_5S, TimeUnit.MILLISECONDS)) {
            loadGeneratorService.shutdownNow();
        }

        int numStreamRecords = 2 * this.numItemsInSrcTable;
        int remainingRecordsToBeProcessed = numStreamRecords - numRecordsProcessedByFirstWorker + numRecordsNotCheckpointed;

        /*
         * The second worker must process atleast remainingRecordsToBeProcessed
         * num of records so that we have replicated everything to destination
         * table. Thus, this should never technically end up as an infinite
         * loop. If it does, something else is gone wrong.
         */
        while (recordProcessorFactory.getNumRecordsProcessed() < remainingRecordsToBeProcessed) {
            LOG.info("Sleep till remaining records are processed");
            Thread.sleep(THREAD_SLEEP_2S);
        }

        shutDownKCLWorker();

        ScanResult srcTableScan = TestUtil.scanTable(dynamoDBClient, srcTable);
        ScanResult destTableScan = TestUtil.scanTable(dynamoDBClient, destTable);
        assertEquals(srcTableScan.getItems(), destTableScan.getItems());
    }

    /**
     * This method will insert items sequentially with hash keys 101, 102 and so on. The updateItem call adds a new
     * attribute to the previously inserted item
     *
     * @param numItemsToInsert
     */
    private void insertAndUpdateItems(int numItemsToInsert) {
        for (int i = 1; i <= numItemsToInsert; i++) {
            numItemsInSrcTable++;
            String partitionKey = Integer.toString(100 + numItemsInSrcTable);
            String attribute1 = partitionKey + "-attr1";
            String attribute2 = partitionKey + "-attr2";

            TestUtil.putItem(dynamoDBClient, srcTable, partitionKey, attribute1);
            TestUtil.updateItem(dynamoDBClient, srcTable, partitionKey, attribute2);
        }
    }
}
