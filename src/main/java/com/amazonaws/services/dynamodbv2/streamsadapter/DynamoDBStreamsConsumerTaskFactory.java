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
package com.amazonaws.services.dynamodbv2.streamsadapter;

import com.amazonaws.services.dynamodbv2.streamsadapter.tasks.DynamoDBStreamsShutdownTask;
import software.amazon.kinesis.lifecycle.ConsumerTask;
import software.amazon.kinesis.lifecycle.KinesisConsumerTaskFactory;
import software.amazon.kinesis.lifecycle.ShardConsumer;
import software.amazon.kinesis.lifecycle.ShardConsumerArgument;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;

@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public class DynamoDBStreamsConsumerTaskFactory extends KinesisConsumerTaskFactory {

    @Override
    public ConsumerTask createShutdownTask(ShardConsumerArgument argument, ShardConsumer consumer, ProcessRecordsInput
            input) {
        return new DynamoDBStreamsShutdownTask(
                argument.shardInfo(),
                argument.shardDetector(),
                argument.shardRecordProcessor(),
                argument.recordProcessorCheckpointer(),
                consumer.shutdownReason(),
                argument.initialPositionInStream(),
                argument.cleanupLeasesOfCompletedShards(),
                argument.ignoreUnexpectedChildShards(),
                argument.leaseCoordinator(),
                argument.taskBackoffTimeMillis(),
                argument.recordsPublisher(),
                argument.hierarchicalShardSyncer(),
                argument.metricsFactory(),
                input == null ? null : input.childShards(),
                argument.streamIdentifier(),
                argument.leaseCleanupManager());
    }
}
