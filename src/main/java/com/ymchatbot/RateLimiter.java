package com.ymchatbot;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class RateLimiter {
    private final AtomicReference<Bucket> secondBucketRef;
    private final AtomicReference<Bucket> minuteBucketRef;

    @Autowired
    public RateLimiter(
        @Value("${application.rate-limit.script.seconds:50000}") int maxMessagesPerSecond,
        @Value("${application.rate-limit.script.minutes:1000000}") int maxMessagesPerMinute
    ) {
        this.secondBucketRef = new AtomicReference<>(createSecondBucket(maxMessagesPerSecond));
        this.minuteBucketRef = new AtomicReference<>(createMinuteBucket(maxMessagesPerMinute));

        LoggerUtil.info(String.format(
            "RateLimiter initialized with: %d messages/second, %d messages/minute",
            maxMessagesPerSecond,
            maxMessagesPerMinute
        ));
    }

    private Bucket createSecondBucket(int maxMessagesPerSecond) {
        Bandwidth limit = Bandwidth.classic(
            maxMessagesPerSecond, 
            Refill.greedy(maxMessagesPerSecond, Duration.ofSeconds(1))
        );
        
        return Bucket.builder()
            .addLimit(limit)
            .build();
    }

    private Bucket createMinuteBucket(int maxMessagesPerMinute) {
        Bandwidth limit = Bandwidth.classic(
            maxMessagesPerMinute, 
            Refill.greedy(maxMessagesPerMinute, Duration.ofMinutes(1))
        );
        
        return Bucket.builder()
            .addLimit(limit)
            .build();
    }

    public boolean tryAcquire() {
        Bucket secondBucket = secondBucketRef.get();
        Bucket minuteBucket = minuteBucketRef.get();

        return secondBucket.tryConsume(1) && minuteBucket.tryConsume(1);
    }

    public boolean tryAcquireWithLogging() {
        Bucket secondBucket = secondBucketRef.get();
        Bucket minuteBucket = minuteBucketRef.get();

        if (!secondBucket.tryConsume(1)) {
            LoggerUtil.debug("Rate limit exceeded for second window");
            return false;
        }

        if (!minuteBucket.tryConsume(1)) {
            LoggerUtil.debug("Rate limit exceeded for minute window");
            return false;
        }

        return true;
    }

    public void updateRateLimits(int newMaxMessagesPerSecond, int newMaxMessagesPerMinute) {
        newMaxMessagesPerSecond = Math.max(1, newMaxMessagesPerSecond);
        newMaxMessagesPerMinute = Math.max(1, newMaxMessagesPerMinute);
        
        secondBucketRef.set(createSecondBucket(newMaxMessagesPerSecond));
        minuteBucketRef.set(createMinuteBucket(newMaxMessagesPerMinute));
        
        LoggerUtil.info(String.format(
            "Rate limits updated: %d messages/second, %d messages/minute", 
            newMaxMessagesPerSecond, 
            newMaxMessagesPerMinute
        ));
    }
}