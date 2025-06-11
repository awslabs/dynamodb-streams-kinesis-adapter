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

import software.amazon.kinesis.retrieval.polling.SleepTimeController;
import software.amazon.kinesis.retrieval.polling.SleepTimeControllerConfig;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public class DynamoDBStreamsSleepTimeController implements SleepTimeController {

    @Override
    public long getSleepTimeMillis(SleepTimeControllerConfig sleepTimeControllerConfig) {
        long idleMillsBetweenCalls = sleepTimeControllerConfig.idleMillisBetweenCalls();
        Instant lastSuccessfulCall = sleepTimeControllerConfig.lastSuccessfulCall();

        Integer lastGetRecordsReturnedRecordsCount = sleepTimeControllerConfig.lastRecordsCount();

        if (lastSuccessfulCall == null || lastGetRecordsReturnedRecordsCount == null) {
            return idleMillsBetweenCalls;
        }
        long sleepTime = lastGetRecordsReturnedRecordsCount > 0 ? idleMillsBetweenCalls : 2 * idleMillsBetweenCalls;
        long timeSinceLastCall = Duration.between(lastSuccessfulCall, Instant.now()).abs().toMillis();
        if (timeSinceLastCall < sleepTime) {
            return sleepTime - timeSinceLastCall;
        }
        return 0;
    }
}
