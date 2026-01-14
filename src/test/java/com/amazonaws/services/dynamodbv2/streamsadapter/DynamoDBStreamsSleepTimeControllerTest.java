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

import com.amazonaws.services.dynamodbv2.streamsadapter.polling.DynamoDBStreamsCatchUpConfig;
import org.junit.Test;
import org.junit.Before;
import software.amazon.kinesis.metrics.MetricsFactory;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.retrieval.polling.SleepTimeControllerConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;

public class DynamoDBStreamsSleepTimeControllerTest {

    private DynamoDBStreamsSleepTimeController controller;
    private MetricsFactory metricsFactory;
    private MetricsScope metricsScope;
    private static final long IDLE_MILLIS = 1000L;

    @Before
    public void setup() {
        metricsFactory = mock(MetricsFactory.class);
        metricsScope = mock(MetricsScope.class);
        when(metricsFactory.createMetrics()).thenReturn(metricsScope);
        controller = new DynamoDBStreamsSleepTimeController(metricsFactory);
    }
    @Test
    public void testGetSleepTimeMillis_WithRecords_ReturnsIdleTime() {
        // When records are returned, sleep time should be equal to idleMillsBetweenCalls
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastSuccessfulCall(Instant.now().minus(Duration.ofMillis(500))) // last call was 500ms ago
                .lastRecordsCount(10)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(IDLE_MILLIS - 500, sleepTime, 100); // Allow small timing differences
    }

    @Test
    public void testGetSleepTimeMillis_NoRecords_ReturnsDoubleIdleTime() {
        // When no records are returned, sleep time should be double the idleMillsBetweenCalls
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastSuccessfulCall(Instant.now().minus(Duration.ofMillis(500))) // last call was 500ms ago
                .lastRecordsCount(0)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(2 * IDLE_MILLIS - 500, sleepTime, 100); // Allow small timing differences
    }
    @Test
    public void testGetSleepTimeMillis_NullLastCall_ReturnsFullSleepTime() {
        // When lastSuccessfulCall is null, should return full sleep time
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastSuccessfulCall(null)
                .lastRecordsCount(10)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTimeWithRecords = controller.getSleepTimeMillis(sleepTimeControllerConfig);

        assertEquals(IDLE_MILLIS, sleepTimeWithRecords);
        sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(null)
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastRecordsCount(0)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTimeNoRecords = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(IDLE_MILLIS, sleepTimeNoRecords);
    }

    @Test
    public void testGetSleepTimeMillis_TimeSinceLastCallExceedsSleepTime_ReturnsZero() {
        // When time since last call exceeds sleep time, should return 0
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastSuccessfulCall(Instant.now().minus(Duration.ofMillis(3000))) // last call was 3000ms ago
                .lastRecordsCount(10)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTime = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        assertEquals(0, sleepTime);
    }

    @Test
    public void testGetSleepTimeMillis_MillisBehindLatestIgnored() {
        // Verify that millisBehindLatest parameter doesn't affect the result
        SleepTimeControllerConfig sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(Instant.now().minus(Duration.ofMillis(500)))
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastRecordsCount(10)
                .lastMillisBehindLatest(1000L)
                .build();
        long sleepTime1 = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        sleepTimeControllerConfig = SleepTimeControllerConfig.builder()
                .lastSuccessfulCall(Instant.now().minus(Duration.ofMillis(500)))
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastRecordsCount(10)
                .lastMillisBehindLatest(0L)
                .build();
        long sleepTime2 = controller.getSleepTimeMillis(sleepTimeControllerConfig);
        // Both should be approximately the same
        assertEquals(sleepTime1, sleepTime2, 100);
    }

    @Test
    public void testCatchUpModeActivation() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig()
                .catchupEnabled(true)
                .scalingFactor(2);

        DynamoDBStreamsSleepTimeController catchUpController =
                new DynamoDBStreamsSleepTimeController(config, metricsFactory);

        SleepTimeControllerConfig sleepConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastMillisBehindLatest(Duration.ofMinutes(10).toMillis())
                .lastSuccessfulCall(null) // No previous call
                .lastRecordsCount(5)
                .build();

        long sleepTime = catchUpController.getSleepTimeMillis(sleepConfig);
        assertEquals(500L, sleepTime); // 1000ms / 2 = 500ms (2x faster)
    }

    @Test
    public void testCatchUpModeDisabled() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig(); // disabled by default

        DynamoDBStreamsSleepTimeController catchUpController =
                new DynamoDBStreamsSleepTimeController(config, metricsFactory);

        SleepTimeControllerConfig sleepConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastMillisBehindLatest(Duration.ofMinutes(10).toMillis())
                .lastSuccessfulCall(null) // No previous call
                .lastRecordsCount(5)
                .build();

        long sleepTime = catchUpController.getSleepTimeMillis(sleepConfig);
        assertEquals(IDLE_MILLIS, sleepTime); // Normal behavior: full idle time
    }

    @Test
    public void testCatchUpModeWithInvalidScalingFactor() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DynamoDBStreamsCatchUpConfig().scalingFactor(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new DynamoDBStreamsCatchUpConfig().scalingFactor(-1);
        });
    }

    @Test
    public void testCatchUpModeWithInvalidThreshold() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DynamoDBStreamsCatchUpConfig().millisBehindLatestThreshold(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new DynamoDBStreamsCatchUpConfig().millisBehindLatestThreshold(-1);
        });
    }

    @Test
    public void testCatchUpModeMetricEmitted() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig()
                .catchupEnabled(true)
                .millisBehindLatestThreshold(60000);

        DynamoDBStreamsSleepTimeController catchUpController =
                new DynamoDBStreamsSleepTimeController(config, metricsFactory);

        SleepTimeControllerConfig sleepConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastMillisBehindLatest(Duration.ofMinutes(5).toMillis())
                .lastSuccessfulCall(null)
                .lastRecordsCount(5)
                .build();

        catchUpController.getSleepTimeMillis(sleepConfig);
        
        // Verify metrics were created
        verify(metricsFactory).createMetrics();
    }

    @Test
    public void testCatchUpModeMetricNotEmittedWhenDisabled() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig()
                .catchupEnabled(false);

        DynamoDBStreamsSleepTimeController catchUpController =
                new DynamoDBStreamsSleepTimeController(config, metricsFactory);

        SleepTimeControllerConfig sleepConfig = SleepTimeControllerConfig.builder()
                .idleMillisBetweenCalls(IDLE_MILLIS)
                .lastMillisBehindLatest(Duration.ofMinutes(5).toMillis())
                .lastSuccessfulCall(null)
                .lastRecordsCount(5)
                .build();

        catchUpController.getSleepTimeMillis(sleepConfig);
        
        // Verify metrics were NOT created
        verify(metricsFactory, never()).createMetrics();
    }
}