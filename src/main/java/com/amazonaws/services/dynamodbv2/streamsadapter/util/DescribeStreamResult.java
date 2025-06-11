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
package com.amazonaws.services.dynamodbv2.streamsadapter.util;

import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.services.kinesis.model.Shard;
import java.util.ArrayList;
import java.util.List;

@Getter
public class DescribeStreamResult {
    private final List<Shard> shards;
    @Setter
    private String streamStatus;

    public DescribeStreamResult() {
        this.shards = new ArrayList<>();
        this.streamStatus = "DISABLED";
    }

    public void addStatus(String streamStatus) {
        this.streamStatus = streamStatus;
    }

    public void addShards(List<Shard> shardList) {
        this.shards.addAll(shardList);
    }

}
