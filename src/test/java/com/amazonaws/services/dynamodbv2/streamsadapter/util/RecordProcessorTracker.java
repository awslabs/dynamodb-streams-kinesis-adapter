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
