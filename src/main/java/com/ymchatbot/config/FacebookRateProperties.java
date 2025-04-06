package com.ymchatbot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "facebook.rate")
public class FacebookRateProperties {

    /**
     * Limite de requisições permitidas.
     */
    private int limit = 10;

    /**
     * Intervalo (em segundos) para renovação do limite.
     */
    private int interval = 1;

    // Getters e Setters
    public int getLimit() {
        return limit;
    }
    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getInterval() {
        return interval;
    }
    public void setInterval(int interval) {
        this.interval = interval;
    }
}
