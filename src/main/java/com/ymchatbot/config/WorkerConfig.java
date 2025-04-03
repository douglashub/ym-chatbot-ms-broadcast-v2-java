package com.ymchatbot.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkerConfig {
    @Value("${application.worker.send-message.replicas}")
    private int sendMessageReplicas;

    @Value("${application.worker.send-message.scheduled-fetch-limit}")
    private int scheduledFetchLimit;

    @Value("${application.worker.send-message.enable-campaign-locking}")
    private boolean enableCampaignLocking;

    @Value("${application.worker.update-status.concurrent-consumers}")
    private int updateStatusConcurrentConsumers;

    // Getters
    public int getSendMessageReplicas() {
        return sendMessageReplicas;
    }

    public int getScheduledFetchLimit() {
        return scheduledFetchLimit;
    }

    public boolean isEnableCampaignLocking() {
        return enableCampaignLocking;
    }

    public int getUpdateStatusConcurrentConsumers() {
        return updateStatusConcurrentConsumers;
    }
}