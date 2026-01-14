package com.amazonaws.services.dynamodbv2.streamsadapter.polling;

import org.junit.jupiter.api.Test;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDBStreamsCatchUpConfigTest {

    @Test
    void testDefaultValues() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        assertFalse(config.catchupEnabled());
        assertEquals(60000, config.millisBehindLatestThreshold());
        assertEquals(3, config.scalingFactor());
    }

    @Test
    void testEnabledMethod() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        config.catchupEnabled(true);
        assertTrue(config.catchupEnabled());
        
        config.catchupEnabled(false);
        assertFalse(config.catchupEnabled());
    }

    @Test
    void testValidThreshold() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        config.millisBehindLatestThreshold(Duration.ofSeconds(1).toMillis());
        assertEquals(1000, config.millisBehindLatestThreshold());
        
        config.millisBehindLatestThreshold(Duration.ofHours(1).toMillis());
        assertEquals(3600000, config.millisBehindLatestThreshold());
    }

    @Test
    void testZeroThresholdThrowsException() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.millisBehindLatestThreshold(Duration.ofNanos(1).toMillis())
        );
        assertEquals("millisBehindLatestThreshold must be positive, got: 0", exception.getMessage());
    }

    @Test
    void testNegativeThresholdThrowsException() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.millisBehindLatestThreshold(-5000)
        );
        assertEquals("millisBehindLatestThreshold must be positive, got: -5000", exception.getMessage());
    }

    @Test
    void testValidScalingFactor() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        config.scalingFactor(1);
        assertEquals(1, config.scalingFactor());
        
        config.scalingFactor(10);
        assertEquals(10, config.scalingFactor());
    }

    @Test
    void testZeroScalingFactorThrowsException() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.scalingFactor(0)
        );
        assertEquals("scalingFactor must be positive, got: 0", exception.getMessage());
    }

    @Test
    void testNegativeScalingFactorThrowsException() {
        DynamoDBStreamsCatchUpConfig config = new DynamoDBStreamsCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.scalingFactor(-1)
        );
        assertEquals("scalingFactor must be positive, got: -1", exception.getMessage());
    }
}
