/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

/**
 * This record processor is intended for use with the DynamoDB Streams Adapter for the
 * Amazon Kinesis Client Library (KCL). It will retrieve the underlying Streams records
 * from the KCL adapter in order to simplify record processing tasks.
 */
public abstract class StreamsRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(StreamsRecordProcessor.class);

    /**
     * {@inheritDoc}
     */
    public abstract void initialize(InitializationInput initializationInput);

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        final List<com.amazonaws.services.dynamodbv2.model.Record> streamsRecords = new ArrayList<com.amazonaws.services.dynamodbv2.model.Record>();
        if (processRecordsInput.getRecords() == null) {
            LOG.warn("ProcessRecordsInput's list of Records was null. Skipping.");
            return;
        }
        for (Record record : processRecordsInput.getRecords()) {
            if (record instanceof RecordAdapter) {
                streamsRecords.add(((RecordAdapter) record).getInternalObject());
            } else {
                // This record processor is not being used with the
                // DynamoDB Streams Adapter for Amazon Kinesis Client
                // Library, so we cannot retrieve any Streams records.
                throw new IllegalArgumentException("Record is not an instance of RecordAdapter");
            }
        }
        processStreamsRecords(streamsRecords, processRecordsInput.getCheckpointer());
    }

    /**
     * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
     * application.
     * Upon fail over, the new instance will get records with sequence number &gt; checkpoint position
     * for each partition key.
     *
     * @param records      Data records to be processed
     * @param checkpointer RecordProcessor should use this instance to checkpoint their progress.
     */
    public abstract void processStreamsRecords(List<com.amazonaws.services.dynamodbv2.model.Record> records, IRecordProcessorCheckpointer checkpointer);

    /**
     * {@inheritDoc}
     */
    public abstract void shutdown(ShutdownInput shutdownInput);

}
