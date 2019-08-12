/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * This implementation of IRecordProcessorFactory creates a variety of
 * record processors for different testing purposes. The type of processor
 * to be created is determined by the constructor.
 */
public class TestRecordProcessorFactory implements IRecordProcessorFactory {

    /**
     * The types of record processors which can be created by this factory.
     */
    private enum Processor {
        REPLICATING, COUNTING
    }


    private Processor requestedProcessor;
    private RecordProcessorTracker tracker;
    private IRecordProcessor createdProcessor = null;

    /**
     * Using this constructor will result in the createProcessor method
     * returning a CountingRecordProcessor.
     *
     * @param tracker RecordProcessorTracker to keep track of the number of
     *                processed records per shard
     */
    public TestRecordProcessorFactory(RecordProcessorTracker tracker) {
        this.tracker = tracker;
        requestedProcessor = Processor.COUNTING;
    }

    private String tableName;
    private com.amazonaws.services.dynamodbv2.AmazonDynamoDB dynamoDB;

    /**
     * Using this constructor will result in the createProcessor method
     * returning a ReplicatingRecordProcessor.
     *
     * @param credentials      AWS credentials used to access DynamoDB
     * @param dynamoDBEndpoint DynamoDB endpoint
     * @param serviceName      Used to initialize the DynamoDB client
     * @param tableName        The name of the table used for replication
     */
    public TestRecordProcessorFactory(com.amazonaws.auth.AWSCredentialsProvider credentials, String dynamoDBEndpoint, String serviceName, String tableName) {
        this.tableName = tableName;
        requestedProcessor = Processor.REPLICATING;

        this.dynamoDB = new AmazonDynamoDBClient(credentials);
        dynamoDB.setEndpoint(dynamoDBEndpoint);
        ((AmazonDynamoDBClient) dynamoDB).setServiceNameIntern(serviceName);
    }

    /**
     * Using this constructor creates a replicating processor for an
     * embedded(in-memory) instance of DynamoDB local
     *
     * @param dynamoDB  DynamoDB client for embedded DynamoDB instance
     * @param tableName The name of the table used for replication
     */
    public TestRecordProcessorFactory(com.amazonaws.services.dynamodbv2.AmazonDynamoDB dynamoDB, String tableName) {
        this.tableName = tableName;
        this.dynamoDB = dynamoDB;
        requestedProcessor = Processor.REPLICATING;
    }

    @Override
    public IRecordProcessor createProcessor() {
        switch (requestedProcessor) {
            case REPLICATING:
                createdProcessor = new ReplicatingRecordProcessor(dynamoDB, tableName);
                break;
            case COUNTING:
                createdProcessor = new CountingRecordProcessor(tracker);
                break;
            default:
                createdProcessor = new CountingRecordProcessor(tracker);
                break;
        }

        return createdProcessor;
    }

    /**
     * This method returns -1 under the following conditions:
     * 1. createProcessor() has not yet been called
     * 2. initialize() method on the ReplicatingRecordProcessor instance has not yet been called
     * 3. requestedProcessor is COUNTING
     *
     * @return number of records processed by processRecords
     */
    public int getNumRecordsProcessed() {
        if (createdProcessor == null)
            return -1;
        switch (requestedProcessor) {
            case REPLICATING:
                return ((ReplicatingRecordProcessor) createdProcessor).getNumRecordsProcessed();
            default:
                return -1;
        }
    }

    public int getNumProcessRecordsCalls() {
        if (createdProcessor == null)
            return -1;
        switch (requestedProcessor) {
            case REPLICATING:
                return ((ReplicatingRecordProcessor) createdProcessor).getNumProcessRecordsCalls();
            default:
                return -1;
        }
    }

}
