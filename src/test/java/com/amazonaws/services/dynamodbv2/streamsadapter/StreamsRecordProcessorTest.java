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
package com.amazonaws.services.dynamodbv2.streamsadapter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;

public class StreamsRecordProcessorTest {

    private Record testRecord;
    private IRecordProcessor recordProcessor;

    @Before
    public void setUp() {
        recordProcessor = new SimpleStreamsRecordProcessor();
    }

    @Test
    public void testProcessRecordsSuccess() throws IOException {
        testRecord = new Record().withDynamodb(new StreamRecord())
                .withEventID("test")
                .withEventName("MODIFY");
        RecordAdapter adapter = new RecordAdapter(testRecord);
        List<com.amazonaws.services.kinesis.model.Record> recordList =
                new ArrayList<com.amazonaws.services.kinesis.model.Record>();
        recordList.add(adapter);
        recordProcessor.processRecords(recordList, null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testProcessRecordsFail() {
        List<com.amazonaws.services.kinesis.model.Record> recordList =
                new ArrayList<com.amazonaws.services.kinesis.model.Record>();
        recordList.add(new com.amazonaws.services.kinesis.model.Record());
        recordProcessor.processRecords(recordList, null);
    }

    private class SimpleStreamsRecordProcessor extends StreamsRecordProcessor {

        @Override
        public void initialize(String shardId) {

        }

        @Override
        public void processStreamsRecords(List<Record> records,
                IRecordProcessorCheckpointer checkpointer) {
            assertEquals(testRecord, records.get(0));
        }

        @Override
        public void shutdown(IRecordProcessorCheckpointer checkpointer,
                ShutdownReason reason) {

        }

    }

}
