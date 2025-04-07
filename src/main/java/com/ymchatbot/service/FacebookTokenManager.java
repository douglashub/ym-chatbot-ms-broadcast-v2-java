package com.ymchatbot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ymchatbot.config.FacebookClientProperties;
import com.ymchatbot.config.FacebookTokenProperties;
import com.ymchatbot.util.LoggerUtil;
import io.micrometer.observation.annotation.Observed;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.*;

@Component
public class FacebookTokenManager {

    private final Cache<String, String> tokenCache;
    private final ConcurrentMap<String, CompletableFuture<String>> tokenRenewalInProgress;
    private final ObjectMapper objectMapper;
    private final CloseableHttpAsyncClient httpClient;
    private final DataSource dataSource;
    private final FacebookClientProperties clientProperties;
    private final FacebookTokenProperties tokenProperties;

    @Autowired
    public FacebookTokenManager(ObjectMapper objectMapper, 
                                CloseableHttpAsyncClient httpClient,
                                DataSource dataSource,
                                FacebookClientProperties clientProperties,
                                FacebookTokenProperties tokenProperties) {
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
        this.dataSource = dataSource;
        this.clientProperties = clientProperties;
        this.tokenProperties = tokenProperties;
        this.tokenCache = Caffeine.newBuilder()
                .expireAfterWrite(tokenProperties.getCacheMinutes(), TimeUnit.MINUTES)
                .maximumSize(1000)
                .build();
        this.tokenRenewalInProgress = new ConcurrentHashMap<>();
    }

    @Observed(name = "facebook.token.get_valid_token", 
              contextualName = "get-valid-token-for-page", 
              lowCardinalityKeyValues = {"operation", "validate_token"})
    public CompletableFuture<String> getValidTokenForPage(String pageId) {
        LoggerUtil.info("Validating token for page: " + pageId);
        return getTokenForPage(pageId)
                .thenCompose(token -> {
                    if (isTokenValid(token)) {
                        LoggerUtil.info("Token is valid for page: " + pageId);
                        return CompletableFuture.completedFuture(token);
                    }
                    LoggerUtil.warn("Token is invalid for page: " + pageId + ". Invalidating and refreshing...");
                    invalidateToken(pageId);
                    return getTokenForPage(pageId);
                });
    }

    @Observed(name = "facebook.token.get_token", 
              contextualName = "get-token-for-page",
              lowCardinalityKeyValues = {"operation", "get_token"})
    public CompletableFuture<String> getTokenForPage(String pageId) {
        String cachedToken = tokenCache.getIfPresent(pageId);
        if (cachedToken != null && !cachedToken.isEmpty()) {
            LoggerUtil.info("Using cached token for page: " + pageId);
            return CompletableFuture.completedFuture(cachedToken);
        }
        
        return tokenRenewalInProgress.computeIfAbsent(pageId, pid ->
            fetchTokenFromDatabase(pid)
                .thenCompose(dbToken -> {
                    if (dbToken == null || dbToken.isEmpty()) {
                        LoggerUtil.info("No token found in DB for page: " + pid + ". Refreshing token...");
                        return refreshTokenWithRetry(pid, tokenProperties.getRetryMax(), tokenProperties.getRetryDelay());
                    } else {
                        LoggerUtil.info("Token fetched from DB for page: " + pid);
                        tokenCache.put(pid, dbToken);
                        return CompletableFuture.completedFuture(dbToken);
                    }
                })
                .whenComplete((result, ex) -> tokenRenewalInProgress.remove(pid))
        );
    }

    @Observed(name = "facebook.token.fetch_from_db", 
              contextualName = "fetch-token-from-database",
              lowCardinalityKeyValues = {"operation", "db_fetch"})
    private CompletableFuture<String> fetchTokenFromDatabase(String pageId) {
        return CompletableFuture.supplyAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                     "SELECT page_access_token FROM facebook_rx_fb_page_info WHERE page_id = ?")) {
                stmt.setString(1, pageId);
                ResultSet rs = stmt.executeQuery();
                if (rs.next()) {
                    String token = rs.getString("page_access_token");
                    LoggerUtil.info("Fetched token from DB for page: " + pageId);
                    return token;
                } else {
                    LoggerUtil.warn("No token found in DB for page: " + pageId);
                    return "";
                }
            } catch (SQLException e) {
                LoggerUtil.error("Error fetching token from DB for page: " + pageId, e);
                throw new CompletionException(e);
            }
        });
    }

    @Observed(name = "facebook.token.refresh", 
              contextualName = "refresh-token",
              lowCardinalityKeyValues = {"operation", "refresh"})
    private CompletableFuture<String> refreshToken(String pageId) {
        LoggerUtil.info("Refreshing token for page: " + pageId);
        CompletableFuture<String> future = new CompletableFuture<>();
        String url = "https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token"
                + "&client_id=" + clientProperties.getId()
                + "&client_secret=" + clientProperties.getSecret()
                + "&fb_exchange_token=" + clientProperties.getRefreshToken();
        
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Content-Type", "application/json");
        
        try {
            httpClient.execute(httpPost, new FutureCallback<org.apache.http.HttpResponse>() {
                @Override
                public void completed(org.apache.http.HttpResponse result) {
                    try {
                        HttpEntity entity = result.getEntity();
                        String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                        ObjectNode responseJson = (ObjectNode) objectMapper.readTree(responseBody);
                        String newToken = responseJson.get("access_token").asText();
                        
                        tokenCache.put(pageId, newToken);
                        updateTokenInDatabase(pageId, newToken);
                        
                        LoggerUtil.info("Token refreshed successfully for page: " + pageId);
                        future.complete(newToken);
                    } catch (Exception e) {
                        LoggerUtil.error("Error processing token refresh response for page: " + pageId, e);
                        future.completeExceptionally(e);
                    }
                }

                @Override
                public void failed(Exception ex) {
                    LoggerUtil.error("Token refresh failed for page: " + pageId, ex);
                    future.completeExceptionally(ex);
                }

                @Override
                public void cancelled() {
                    LoggerUtil.warn("Token refresh cancelled for page: " + pageId);
                    future.completeExceptionally(new RuntimeException("Token refresh cancelled"));
                }
            });
        } catch (Exception e) {
            LoggerUtil.error("Exception during token refresh execution for page: " + pageId, e);
            future.completeExceptionally(e);
        }
        return future;
    }

    private CompletableFuture<String> refreshTokenWithRetry(String pageId, int remainingRetries, long delayMs) {
        return refreshToken(pageId)
                .exceptionallyCompose(ex -> {
                    LoggerUtil.warn("Token refresh error for page: " + pageId + ". Retries remaining: " + remainingRetries + ". Error: " + ex.getMessage());
                    if (remainingRetries > 0) {
                        try {
                            Thread.sleep(delayMs);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return CompletableFuture.failedFuture(ie);
                        }
                        return refreshTokenWithRetry(pageId, remainingRetries - 1, delayMs * 2);
                    }
                    return CompletableFuture.failedFuture(ex);
                });
    }

    private void updateTokenInDatabase(String pageId, String newToken) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(
                     "UPDATE facebook_rx_fb_page_info SET page_access_token = ? WHERE page_id = ?")) {
                stmt.setString(1, newToken);
                stmt.setString(2, pageId);
                stmt.executeUpdate();
                LoggerUtil.info("Token updated in DB for page: " + pageId);
            } catch (SQLException e) {
                LoggerUtil.error("Failed to update token in database for page: " + pageId, e);
            }
        });
    }

    private boolean isTokenValid(String token) {
        boolean valid = token != null && token.length() > 30 && token.startsWith("EA") && !token.contains(" ");
        if (!valid) {
            LoggerUtil.warn("Invalid token detected.");
        }
        return valid;
    }

    public void invalidateToken(String pageId) {
        LoggerUtil.info("Invalidating token for page: " + pageId);
        tokenCache.invalidate(pageId);
    }
}
