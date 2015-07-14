/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

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
        REPLICATING,
        COUNTING
    }

    private Processor requestedProcessor;

    private RecordProcessorTracker tracker;

    /**
     * Using this constructor will result in the createProcessor method
     * returning a CountingRecordProcessor.
     * @param tracker RecordProcessorTracker to keep track of the number of
     *      processed records per shard
     */
    public TestRecordProcessorFactory(RecordProcessorTracker tracker) {
        this.tracker = tracker;
        requestedProcessor = Processor.COUNTING;
    }

    private AWSCredentialsProvider credentials;
    private String dynamoDBEndpoint;
    private String serviceName;
    private String tableName;

    /**
     * Using this constructor will result in the createProcessor method
     * returning a ReplicatingRecordProcessor.
     * @param credentials AWS credentials used to access DynamoDB
     * @param dynamoDBEndpoint DynamoDB endpoint
     * @param serviceName Used to initialize the DynamoDB client
     * @param tableName The name of the table used for replication
     */
    public TestRecordProcessorFactory(
            AWSCredentialsProvider credentials,
            String dynamoDBEndpoint,
            String serviceName,
            String tableName) {
        this.credentials = credentials;
        this.dynamoDBEndpoint = dynamoDBEndpoint;
        this.serviceName = serviceName;
        this.tableName = tableName;
        requestedProcessor = Processor.REPLICATING;
    }

    @Override
    public IRecordProcessor createProcessor() {
        switch(requestedProcessor) {
        case REPLICATING :
            AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient(credentials);
            dynamoDBClient.setEndpoint(dynamoDBEndpoint);
            dynamoDBClient.setServiceNameIntern(serviceName);
            return new ReplicatingRecordProcessor(dynamoDBClient, tableName);
        case COUNTING :
            return new CountingRecordProcessor(tracker);
        default :
            return new CountingRecordProcessor(tracker);
        }
    }

}
