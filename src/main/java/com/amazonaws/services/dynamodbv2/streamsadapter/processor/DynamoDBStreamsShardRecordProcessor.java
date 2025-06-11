/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.dynamodbv2.streamsadapter.processor;

import com.amazonaws.services.dynamodbv2.streamsadapter.adapter.DynamoDBStreamsClientRecord;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DynamoDBStreamsProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import java.util.stream.Collectors;

public interface DynamoDBStreamsShardRecordProcessor extends ShardRecordProcessor {
    @Override
    default void processRecords(ProcessRecordsInput processRecordsInput) {
        DynamoDBStreamsProcessRecordsInput dynamoDBStreamsProcessRecordsInput = DynamoDBStreamsProcessRecordsInput
                .builder()
                .records(processRecordsInput.records().stream()
                        .map(kinesisClientRecord -> (DynamoDBStreamsClientRecord) kinesisClientRecord)
                        .collect(Collectors.toList()))
                .checkpointer(processRecordsInput.checkpointer())
                .cacheEntryTime(processRecordsInput.cacheEntryTime())
                .cacheExitTime(processRecordsInput.cacheExitTime())
                .millisBehindLatest(processRecordsInput.millisBehindLatest())
                .childShards(processRecordsInput.childShards())
                .isAtShardEnd(processRecordsInput.isAtShardEnd())
                .build();
        processRecords(dynamoDBStreamsProcessRecordsInput);
    }

    void processRecords(DynamoDBStreamsProcessRecordsInput processRecordsInput);
}
