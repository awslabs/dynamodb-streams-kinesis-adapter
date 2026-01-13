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

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsLevel;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.metrics.MetricsUtil;
import software.amazon.kinesis.retrieval.polling.SleepTimeController;
import software.amazon.kinesis.retrieval.polling.SleepTimeControllerConfig;
import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsClientSideCatchUpConfig;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@SuppressWarnings("checkstyle:AvoidInlineConditionals")
public class DynamoDBStreamsSleepTimeController implements SleepTimeController {

    private static final String OPERATION = "ApplicationTracker";
    private static final String CATCHUP_MODE_ACTIVE_METRIC = "DynamoDBStreamsCatchUpModeActive";

    @NonNull
    private final DynamoDBStreamsClientSideCatchUpConfig catchUpConfig;
    @NonNull
    private final MetricsFactory metricsFactory;

    public DynamoDBStreamsSleepTimeController(@NonNull MetricsFactory metricsFactory) {
        this.catchUpConfig = new DynamoDBStreamsClientSideCatchUpConfig();
        this.metricsFactory = metricsFactory;
    }

    public DynamoDBStreamsSleepTimeController(@NonNull DynamoDBStreamsClientSideCatchUpConfig catchUpConfig,
                                              @NonNull MetricsFactory metricsFactory) {
        this.catchUpConfig = catchUpConfig;
        this.metricsFactory = metricsFactory;
    }

    @Override
    public long getSleepTimeMillis(SleepTimeControllerConfig sleepTimeControllerConfig) {
        long idleMillsBetweenCalls = sleepTimeControllerConfig.idleMillisBetweenCalls();

        // Reduce sleep time during catch-up mode
        if (shouldEnterCatchUpMode(sleepTimeControllerConfig)) {
            idleMillsBetweenCalls = idleMillsBetweenCalls / catchUpConfig.scalingFactor();
            log.debug("Catch-up mode enabled: reducing sleep time by factor of {} (millisBehindLatest: {}ms, threshold: {}ms)",
                    catchUpConfig.scalingFactor(),
                    sleepTimeControllerConfig.lastMillisBehindLatest(),
                    catchUpConfig.millisBehindLatestThreshold().toMillis());
            emitCatchUpModeMetric();
        }

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

    /**
     * Determines if catch-up mode should activate based on lastMillisBehindLatest.
     */
    private boolean shouldEnterCatchUpMode(SleepTimeControllerConfig config) {
        return catchUpConfig.catchupEnabled()
                && config.lastMillisBehindLatest() != null
                && config.lastMillisBehindLatest() > catchUpConfig.millisBehindLatestThreshold().toMillis();
    }

    /**
     * Emit metric for catch-up mode activation.
     */
    private void emitCatchUpModeMetric() {
        MetricsScope scope = MetricsUtil.createMetricsWithOperation(metricsFactory, OPERATION);
        try {
            MetricsUtil.addCount(scope, CATCHUP_MODE_ACTIVE_METRIC, 1, MetricsLevel.SUMMARY);
        } finally {
            MetricsUtil.endScope(scope);
        }
    }
}
