package com.ymchatbot.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "facebook.token")
public class FacebookTokenProperties {

    /**
     * Tempo em minutos para expiração do token cache.
     */
    private int cacheMinutes = 55;

    /**
     * Número máximo de tentativas para refresh do token.
     */
    private int retryMax = 3;

    /**
     * Delay inicial entre tentativas (em milissegundos).
     */
    private long retryDelay = 1000;

    // Getters e Setters
    public int getCacheMinutes() {
        return cacheMinutes;
    }
    public void setCacheMinutes(int cacheMinutes) {
        this.cacheMinutes = cacheMinutes;
    }

    public int getRetryMax() {
        return retryMax;
    }
    public void setRetryMax(int retryMax) {
        this.retryMax = retryMax;
    }

    public long getRetryDelay() {
        return retryDelay;
    }
    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }
}
