package com.amazonaws.services.dynamodbv2.streamsadapter.polling;

import org.junit.jupiter.api.Test;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDBStreamsClientSideCatchUpConfigTest {

    @Test
    void testDefaultValues() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        assertFalse(config.catchupEnabled());
        assertEquals(Duration.ofMinutes(1), config.millisBehindLatestThreshold());
        assertEquals(3, config.scalingFactor());
    }

    @Test
    void testEnabledMethod() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        config.catchupEnabled(true);
        assertTrue(config.catchupEnabled());
        
        config.catchupEnabled(false);
        assertFalse(config.catchupEnabled());
    }

    @Test
    void testValidThreshold() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        config.millisBehindLatestThreshold(Duration.ofSeconds(1));
        assertEquals(Duration.ofSeconds(1), config.millisBehindLatestThreshold());
        
        config.millisBehindLatestThreshold(Duration.ofHours(1));
        assertEquals(Duration.ofHours(1), config.millisBehindLatestThreshold());
    }

    @Test
    void testNullThresholdThrowsException() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.millisBehindLatestThreshold(null)
        );
        assertEquals("Threshold cannot be null", exception.getMessage());
    }

    @Test
    void testZeroThresholdThrowsException() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.millisBehindLatestThreshold(Duration.ZERO)
        );
        assertEquals("Threshold must be positive, got: PT0S", exception.getMessage());
    }

    @Test
    void testNegativeThresholdThrowsException() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.millisBehindLatestThreshold(Duration.ofMinutes(-5))
        );
        assertEquals("Threshold must be positive, got: PT-5M", exception.getMessage());
    }

    @Test
    void testValidScalingFactor() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        config.scalingFactor(1);
        assertEquals(1, config.scalingFactor());
        
        config.scalingFactor(10);
        assertEquals(10, config.scalingFactor());
    }

    @Test
    void testZeroScalingFactorThrowsException() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.scalingFactor(0)
        );
        assertEquals("Scaling factor must be positive, got: 0", exception.getMessage());
    }

    @Test
    void testNegativeScalingFactorThrowsException() {
        DynamoDBStreamsClientSideCatchUpConfig config = new DynamoDBStreamsClientSideCatchUpConfig();
        
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> config.scalingFactor(-1)
        );
        assertEquals("Scaling factor must be positive, got: -1", exception.getMessage());
    }
}
