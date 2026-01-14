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
import java.time.Duration;

/**
 * Configuration for automatic polling rate adjustment when stream processing falls behind.
 */
@Accessors(fluent = true)
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class DynamoDBStreamsCatchUpConfig {
    private boolean catchupEnabled = false;
    private long millisBehindLatestThreshold = Duration.ofMinutes(1).toMillis();
    private int scalingFactor = 3;
    
    public DynamoDBStreamsCatchUpConfig millisBehindLatestThreshold(long threshold) {
        if (threshold <= 0) {
            throw new IllegalArgumentException("millisBehindLatestThreshold must be positive, got: " + threshold);
        }
        this.millisBehindLatestThreshold = threshold;
        return this;
    }
    
    public DynamoDBStreamsCatchUpConfig scalingFactor(int scalingFactor) {
        if (scalingFactor <= 0) {
            throw new IllegalArgumentException("scalingFactor must be positive, got: " + scalingFactor);
        }
        this.scalingFactor = scalingFactor;
        return this;
    }
}
