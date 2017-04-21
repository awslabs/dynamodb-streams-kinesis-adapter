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

import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class ReplicatingRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(ReplicatingRecordProcessor.class);

    private com.amazonaws.services.dynamodbv2.AmazonDynamoDB dynamoDBClient;
    private String tableName;
    private Integer checkpointCounter = -1;
    private Integer processRecordsCallCounter;

    public static final int CHECKPOINT_BATCH_SIZE = 10;

    ReplicatingRecordProcessor(com.amazonaws.services.dynamodbv2.AmazonDynamoDB dynamoDBClient, String tableName) {
        this.dynamoDBClient = dynamoDBClient;
        this.tableName = tableName;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        checkpointCounter = 0;
        processRecordsCallCounter = 0;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        processRecordsCallCounter++;
        for (Record record : processRecordsInput.getRecords()) {
            String data = new String(record.getData().array(), Charset.forName("UTF-8"));
            LOG.info("Got record: " + data);
            if (record instanceof RecordAdapter) {
                com.amazonaws.services.dynamodbv2.model.Record usRecord = ((RecordAdapter) record).getInternalObject();
                switch (usRecord.getEventName()) {
                    case "INSERT":
                    case "MODIFY":
                        TestUtil.putItem(dynamoDBClient, tableName, usRecord.getDynamodb().getNewImage());
                        break;
                    case "REMOVE":
                        TestUtil.deleteItem(dynamoDBClient, tableName, usRecord.getDynamodb().getKeys().get("Id").getN());
                        break;
                }
            }
            checkpointCounter += 1;
            if (checkpointCounter % CHECKPOINT_BATCH_SIZE == 0) {
                try {
                    processRecordsInput.getCheckpointer().checkpoint(record.getSequenceNumber());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (ShutdownReason.TERMINATE.equals(shutdownInput.getShutdownReason())) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    int getNumRecordsProcessed() {
        return checkpointCounter;
    }

    int getNumProcessRecordsCalls() {
        return processRecordsCallCounter;
    }

}
