package com.ymchatbot.config;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HttpClientConfig {

    @Bean(destroyMethod = "close")
    public CloseableHttpAsyncClient httpAsyncClient() {
        try {
            DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            connManager.setMaxTotal(200);
            connManager.setDefaultMaxPerRoute(200);
    
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(30000)
                    .setSocketTimeout(30000)
                    .build();
    
            CloseableHttpAsyncClient client = HttpAsyncClients.custom()
                    .setConnectionManager(connManager)
                    .setDefaultRequestConfig(requestConfig)
                    .build();
            client.start();
            return client;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HTTP async client", e);
        }
    }
}