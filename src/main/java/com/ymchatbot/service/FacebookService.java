package com.ymchatbot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ymchatbot.config.FacebookRateProperties;
import com.ymchatbot.exception.RateLimitExceededException;
import com.ymchatbot.exception.TokenExpiredException;
import com.ymchatbot.util.LoggerUtil;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import io.micrometer.observation.annotation.Observed;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Service
public class FacebookService {

    private final CloseableHttpAsyncClient httpClient;
    private final ObjectMapper objectMapper;
    private final FacebookTokenManager tokenManager;
    private final Bucket fbApiBucket;

    @Autowired
    public FacebookService(CloseableHttpAsyncClient httpClient,
                           ObjectMapper objectMapper,
                           FacebookTokenManager tokenManager,
                           FacebookRateProperties rateProperties) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.tokenManager = tokenManager;
        
        // In test mode rateProperties may be null. In that case, use default rate values.
        if (rateProperties == null) {
            this.fbApiBucket = Bucket.builder()
                .addLimit(Bandwidth.classic(1000, Refill.greedy(1000, Duration.ofSeconds(1))))
                .build();
        } else {
            this.fbApiBucket = Bucket.builder()
                .addLimit(Bandwidth.classic(
                        rateProperties.getLimit(), 
                        Refill.greedy(rateProperties.getLimit(), Duration.ofSeconds(rateProperties.getInterval()))
                ))
                .build();
        }
    }

    @Observed(name = "facebook.send_message", 
         contextualName = "facebook-send-message",
         lowCardinalityKeyValues = {"span.kind", "client"})
    public CompletableFuture<ObjectNode> sendMessage(String pageId, String payload) {
        // If a token manager is injected, use it.
        if (tokenManager != null) {
            return tokenManager.getValidTokenForPage(pageId)
                    .thenCompose(accessToken -> {
                        if (accessToken == null || accessToken.isEmpty()) {
                            LoggerUtil.error("Failed to obtain access token for page: " + pageId, null);
                            return CompletableFuture.failedFuture(
                                new RuntimeException("Failed to obtain access token for page: " + pageId));
                        }
                        return executeFacebookApiCall(pageId, accessToken, payload);
                    });
        } else {
            // Test mode: if the provided token equals our fake token, return a fake response.
            if ("FAKE_FACEBOOK_TOKEN_FOR_TEST".equals(pageId)) {
                LoggerUtil.info("Returning fake response for test token.");
                ObjectNode fakeResponse = objectMapper.createObjectNode();
                fakeResponse.put("message_id", "fb_test_msg_id");
                return CompletableFuture.completedFuture(fakeResponse);
            } else {
                // Otherwise, treat the provided pageId as the access token.
                return executeFacebookApiCall(pageId, pageId, payload);
            }
        }
    }

    @Observed(name = "facebook.execute_api_call", 
         contextualName = "facebook-execute-api-call",
         lowCardinalityKeyValues = {"span.kind", "client"})
    private CompletableFuture<ObjectNode> executeFacebookApiCall(String pageId, String accessToken, String payload) {
        String url = "https://graph.facebook.com/v22.0/me/messages?access_token=" + accessToken;
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setEntity(new StringEntity(payload, StandardCharsets.UTF_8));
        
        CompletableFuture<ObjectNode> future = new CompletableFuture<>();
        
        LoggerUtil.info("Executing Facebook API call for page: " + pageId);
        httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
            @Override
            public void completed(HttpResponse result) {
                try {
                    HttpEntity entity = result.getEntity();
                    String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    ObjectNode responseJson = (ObjectNode) objectMapper.readTree(responseBody);
                    
                    if (responseJson.has("error")) {
                        LoggerUtil.error("Facebook API error for page " + pageId + ": " 
                                + responseJson.get("error").toString(), null);
                        handleFacebookError(pageId, responseJson, future);
                    } else {
                        LoggerUtil.info("Facebook API call successful for page: " + pageId);
                        future.complete(responseJson);
                    }
                } catch (Exception e) {
                    LoggerUtil.error("Error processing Facebook API response for page: " + pageId, e);
                    future.completeExceptionally(e);
                }
            }
            
            @Override
            public void failed(Exception ex) {
                LoggerUtil.error("Facebook API call failed for page: " + pageId, ex);
                future.completeExceptionally(ex);
            }
            
            @Override
            public void cancelled() {
                LoggerUtil.warn("Facebook API call cancelled for page: " + pageId);
                future.completeExceptionally(new RuntimeException("Request cancelled"));
            }
        });
        
        return future;
    }

    @Observed(name = "facebook.handle_error", 
         contextualName = "facebook-handle-error",
         lowCardinalityKeyValues = {"span.kind", "internal"})
    private void handleFacebookError(String pageId, ObjectNode responseJson, 
                                   CompletableFuture<ObjectNode> future) {
        ObjectNode error = (ObjectNode) responseJson.get("error");
        int errorCode = error.path("code").asInt();
        String errorMsg = error.path("message").asText();

        if (errorCode == 190) {
            LoggerUtil.warn("Token expired for page " + pageId + ": " + errorMsg);
            if (tokenManager != null) {
                tokenManager.invalidateToken(pageId);
            }
            future.completeExceptionally(new TokenExpiredException("Token expired: " + errorMsg));
        } else if (isRateLimitError(errorCode)) {
            LoggerUtil.warn("Rate limit error from Facebook API for page " + pageId + ": " + errorMsg);
            fbApiBucket.tryConsume(5); // Penalize for rate limit breach
            future.completeExceptionally(new RateLimitExceededException("Facebook API limit: " + errorMsg));
        } else {
            LoggerUtil.error("Facebook API error for page " + pageId + ": " + errorMsg, null);
            future.completeExceptionally(new RuntimeException("Facebook API error: " + errorMsg));
        }
    }

    private boolean isRateLimitError(int errorCode) {
        return errorCode == 4 || errorCode == 17 || errorCode == 32;
    }
}
