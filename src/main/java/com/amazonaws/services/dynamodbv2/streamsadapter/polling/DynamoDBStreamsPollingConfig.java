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
package com.amazonaws.services.dynamodbv2.streamsadapter.polling;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.retrieval.RetrievalFactory;
import software.amazon.kinesis.retrieval.polling.PollingConfig;
import software.amazon.kinesis.retrieval.polling.SynchronousBlockingRetrievalFactory;

@Accessors(fluent = true)
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class DynamoDBStreamsPollingConfig extends PollingConfig {

    private long idleTimeBetweenReadsInMillis = 1000L;

    public DynamoDBStreamsPollingConfig(KinesisAsyncClient kinesisAsyncClient) {
        super(kinesisAsyncClient);
    }

    public DynamoDBStreamsPollingConfig(String streamName, KinesisAsyncClient kinesisAsyncClient) {
        super(streamName, kinesisAsyncClient);
    }

    public DynamoDBStreamsPollingConfig idleTimeBetweenReadsInMillis(long idleTimeBetweenReadsInMillis) {
        this.idleTimeBetweenReadsInMillis = idleTimeBetweenReadsInMillis;
        return this;
    }

    @Override
    public RetrievalFactory retrievalFactory() {
        recordsFetcherFactory().idleMillisBetweenCalls(idleTimeBetweenReadsInMillis);
        return new SynchronousBlockingRetrievalFactory(
                streamName(),
                kinesisClient(),
                recordsFetcherFactory(),
                maxRecords(),
                kinesisRequestTimeout(),
                dataFetcherProvider(),
                sleepTimeController()
        );
    }
}
