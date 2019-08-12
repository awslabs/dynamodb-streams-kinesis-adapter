/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
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
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

public class StreamsRecordProcessorTest {

    private Record testRecord;
    private IRecordProcessor recordProcessor;

    @Before
    public void setUp() {
        recordProcessor = new SimpleStreamsRecordProcessor();
    }

    @Test
    public void testProcessRecordsSuccess() throws IOException {
        testRecord = new Record().withDynamodb(new StreamRecord()).withEventID("test").withEventName("MODIFY");
        RecordAdapter adapter = new RecordAdapter(testRecord);
        List<com.amazonaws.services.kinesis.model.Record> recordList = new ArrayList<com.amazonaws.services.kinesis.model.Record>();
        recordList.add(adapter);
        recordProcessor.processRecords(new ProcessRecordsInput().withRecords(recordList));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testProcessRecordsFail() {
        List<com.amazonaws.services.kinesis.model.Record> recordList = new ArrayList<com.amazonaws.services.kinesis.model.Record>();
        recordList.add(new com.amazonaws.services.kinesis.model.Record());
        recordProcessor.processRecords(new ProcessRecordsInput().withRecords(recordList));
    }

    private class SimpleStreamsRecordProcessor extends StreamsRecordProcessor {

        @Override
        public void initialize(InitializationInput initializationInput) {

        }

        @Override
        public void processStreamsRecords(List<com.amazonaws.services.dynamodbv2.model.Record> records, IRecordProcessorCheckpointer checkpointer) {
            assertEquals(testRecord, records.get(0));
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {

        }

    }

}
