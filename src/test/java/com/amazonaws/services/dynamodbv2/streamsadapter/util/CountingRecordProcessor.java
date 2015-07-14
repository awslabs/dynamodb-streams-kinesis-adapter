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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class CountingRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(CountingRecordProcessor.class);

    private RecordProcessorTracker tracker;

    private String shardId;
    private Integer checkpointCounter;
    private Integer recordCounter;

    public CountingRecordProcessor(RecordProcessorTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        checkpointCounter = 0;
        recordCounter = 0;
    }

    @Override
    public void processRecords(List<Record> records,
            IRecordProcessorCheckpointer checkpointer) {
        for(Record record : records) {
            recordCounter += 1;
            checkpointCounter += 1;
            if(checkpointCounter % 10 == 0) {
                try {
                    checkpointer.checkpoint(record.getSequenceNumber());
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer,
            ShutdownReason reason) {
        if(reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        LOG.info("Processed "+ recordCounter + " records for " + shardId);
        tracker.shardProcessed(shardId, recordCounter);
    }

}
