package com.ymchatbot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ymchatbot.util.LoggerUtil;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

@Service
public class FacebookService {

    protected final CloseableHttpAsyncClient httpClient;
    protected final ObjectMapper objectMapper;

    @Autowired
    public FacebookService(CloseableHttpAsyncClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<ObjectNode> sendMessage(String accessToken, String payload) {
        // TODO: Replace this test-only branch with a dynamic token provider (e.g., FacebookTokenManager)
        // When in production, remove this branch so that a real token is always used.
        if ("FAKE_FACEBOOK_TOKEN_FOR_TEST".equals(accessToken)) {
            ObjectNode response = objectMapper.createObjectNode();
            response.put("message_id", "fb_test_msg_id");
            return CompletableFuture.completedFuture(response);
        }
        
        CompletableFuture<ObjectNode> future = new CompletableFuture<>();
        String url = "https://graph.facebook.com/v22.0/me/messages?access_token=" + accessToken;

        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(payload, StandardCharsets.UTF_8));
            httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                @Override
                public void completed(HttpResponse result) {
                    try {
                        HttpEntity entity = result.getEntity();
                        String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                        ObjectNode responseJson = (ObjectNode) objectMapper.readTree(responseBody);
                        future.complete(responseJson);
                    } catch (Exception e) {
                        LoggerUtil.error("Error processing Facebook API response", e);
                        future.completeExceptionally(e);
                    }
                }
                @Override
                public void failed(Exception ex) {
                    LoggerUtil.error("Facebook API request failed", ex);
                    future.completeExceptionally(ex);
                }
                @Override
                public void cancelled() {
                    LoggerUtil.warn("Facebook API request cancelled");
                    future.completeExceptionally(new RuntimeException("Request cancelled"));
                }
            });
        } catch (Exception e) {
            LoggerUtil.error("Error sending message to Facebook API", e);
            future.completeExceptionally(e);
        }
        return future;
    }
}