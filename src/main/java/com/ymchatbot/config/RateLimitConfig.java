package com.ymchatbot.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimitConfig {
    @Value("${application.rate-limit.script.seconds}")
    private int rateLimitPerSecond;

    @Value("${application.rate-limit.script.minutes}")
    private int rateLimitPerMinute;

    public int getRateLimitPerSecond() {
        return rateLimitPerSecond;
    }

    public int getRateLimitPerMinute() {
        return rateLimitPerMinute;
    }
}