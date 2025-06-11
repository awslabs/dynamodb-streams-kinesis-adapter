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

import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.processor.FormerStreamsLeasesDeletionStrategy;
import software.amazon.kinesis.processor.MultiStreamTracker;
import java.util.List;

public class DynamoDBStreamsMultiStreamTracker implements MultiStreamTracker {

    private final List<StreamConfig> streamConfigList;
    private final FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy;

    public DynamoDBStreamsMultiStreamTracker(List<StreamConfig> streamConfigList,
                                             FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy) {
        this.streamConfigList = streamConfigList;
        this.formerStreamsLeasesDeletionStrategy = formerStreamsLeasesDeletionStrategy;
    }
    @Override
    public List<StreamConfig> streamConfigList() {
        return streamConfigList;
    }

    @Override
    public FormerStreamsLeasesDeletionStrategy formerStreamsLeasesDeletionStrategy() {
        return formerStreamsLeasesDeletionStrategy;
    }
}
