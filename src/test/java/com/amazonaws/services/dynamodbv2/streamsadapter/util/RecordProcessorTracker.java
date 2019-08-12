/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The RecordProcessorTracker keeps track of the number of records
 * per shard which have been successfully processed.
 */
public class RecordProcessorTracker {

    private Set<String> shardIds;
    private volatile Map<String, Integer> processedRecordCounts;

    /**
     * Constructor.
     *
     * @param shardIds The shards which are being processed
     */
    public RecordProcessorTracker(Set<String> shardIds) {
        this.shardIds = shardIds;
        processedRecordCounts = new HashMap<String, Integer>();
    }

    /**
     * Invoked by the IRecordProcessor::shutdown() method.
     *
     * @param shardId The shard ID
     * @param count   The number of records which have been successfully processed
     */
    public void shardProcessed(String shardId, Integer count) {
        processedRecordCounts.put(shardId, count);
    }

    /**
     * Determines if the initially specified shards have all been
     * completely processed.
     *
     * @return True if all shards have been processed, false otherwise
     */
    public boolean isDoneProcessing() {
        Set<String> processedShards;
        synchronized (processedRecordCounts) {
            processedShards = processedRecordCounts.keySet();
        }
        return processedShards.equals(shardIds);
    }

    /**
     * Returns the number of successfully processed records for each shard.
     *
     * @return Number of records processed per shard
     */
    public Map<String, Integer> getProcessedRecordCounts() {
        return processedRecordCounts;
    }

}
