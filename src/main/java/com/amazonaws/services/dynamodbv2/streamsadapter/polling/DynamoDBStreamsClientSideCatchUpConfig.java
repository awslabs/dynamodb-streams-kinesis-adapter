package com.amazonaws.services.dynamodbv2.streamsadapter.polling;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import java.time.Duration;

@Accessors(fluent = true)
@Getter
@ToString
@EqualsAndHashCode
public class DynamoDBStreamsClientSideCatchUpConfig {
    private boolean enabled = false;
    private Duration millisBehindLatestThreshold = Duration.ofMinutes(5);
    private int scalingFactor = 2;
    
    public DynamoDBStreamsClientSideCatchUpConfig enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }
    
    public DynamoDBStreamsClientSideCatchUpConfig millisBehindLatestThreshold(Duration threshold) {
        if (threshold == null) {
            throw new IllegalArgumentException("Threshold cannot be null");
        }
        if (threshold.isNegative() || threshold.isZero()) {
            throw new IllegalArgumentException("Threshold must be positive, got: " + threshold);
        }
        this.millisBehindLatestThreshold = threshold;
        return this;
    }
    
    public DynamoDBStreamsClientSideCatchUpConfig scalingFactor(int scalingFactor) {
        if (scalingFactor <= 0) {
            throw new IllegalArgumentException("Scaling factor must be positive, got: " + scalingFactor);
        }
        this.scalingFactor = scalingFactor;
        return this;
    }
}
