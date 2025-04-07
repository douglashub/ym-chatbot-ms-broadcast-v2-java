
package com.ymchatbot.worker;

// Update imports to be specific
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.annotation.PostConstruct;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Delivery;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.util.EntityUtils;

import com.ymchatbot.FileRateLimiter;
import com.ymchatbot.RateLimiter;
import com.ymchatbot.util.LoggerUtil;
import com.ymchatbot.util.MessageValidator;

import io.micrometer.observation.annotation.Observed;

import com.ymchatbot.config.DatabaseConfig;
import com.ymchatbot.config.RabbitMQConfig;
import com.ymchatbot.config.RateLimitConfig;
import com.ymchatbot.config.WorkerConfig;
import com.ymchatbot.service.FacebookService;

@Component
@EnableScheduling
public class WorkerSendMessage {
    private final String queueName = "broadcast-v2/send-messages";
    private final String updateStatusQueueName = "broadcast-v2/update-status";
    private final int MAX_CONNECTIONS = 200;

    private static final int CAMPAIGN_STATUS_COMPLETED = 2;
    private static final int CAMPAIGN_STATUS_ERROR = 4;
    private static final double ERROR_THRESHOLD = 0.5;

    @Autowired
    private DatabaseConfig databaseConfig;

    @Autowired
    private RabbitMQConfig rabbitMQConfig;

    @Autowired
    private RateLimitConfig rateLimitConfig;

    @Autowired
    private WorkerConfig workerConfig;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private FacebookService facebookService;

    private RateLimiter rateLimiter;
    private java.sql.Connection dbConnection;
    private CloseableHttpAsyncClient httpClient;
    private Channel channel;
    private Channel updateStatusChannel;

    // Improved logging counters
    private static AtomicLong processedMessageCounter = new AtomicLong(0);
    private static AtomicLong inactiveCampaignCounter = new AtomicLong(0);
    private static AtomicLong errorCounter = new AtomicLong(0);
    private static long lastStatsLogTime = 0;
    private static long lastInactiveLogTime = 0;
    private static final long STATS_LOG_INTERVAL_MS = 30000; // Log stats every 30 seconds

    @Autowired
    public WorkerSendMessage() {
        LoggerUtil.info("WorkerSendMessage constructor called");
    }

    @PostConstruct
    public void initialize() throws Exception {
        rateLimiter = new RateLimiter(rateLimitConfig.getRateLimitPerSecond(), rateLimitConfig.getRateLimitPerMinute());

        LoggerUtil
                .info("Starting WorkerSendMessage with rate limits: " + rateLimitConfig.getRateLimitPerSecond() + "s / "
                        + rateLimitConfig.getRateLimitPerMinute() + "m");

        initDbConnection();
        initHttpClient();
        initializeRabbitMQ();

        LoggerUtil.info("WorkerSendMessage initialization complete");
    }

    private void initDbConnection() throws SQLException {
        dbConnection = databaseConfig.dataSource().getConnection();
        LoggerUtil.info("Database connection established");
    }

    private void initHttpClient() throws Exception {
        try {
            DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
            connManager.setMaxTotal(MAX_CONNECTIONS);
            connManager.setDefaultMaxPerRoute(MAX_CONNECTIONS);

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(30000)
                    .setSocketTimeout(30000)
                    .build();

            httpClient = HttpAsyncClients.custom()
                    .setConnectionManager(connManager)
                    .setDefaultRequestConfig(requestConfig)
                    .build();

            httpClient.start();
            LoggerUtil.info("HTTP client initialized with " + MAX_CONNECTIONS + " max connections");
        } catch (Exception e) {
            LoggerUtil.error("Failed to initialize HTTP client", e);
            throw e;
        }
    }

    private void initializeRabbitMQ() throws Exception {
        ConnectionFactory factory = rabbitMQConfig.rabbitConnectionFactory();
        com.rabbitmq.client.Connection connection = factory.newConnection("worker-send-message");

        channel = connection.createChannel();
        channel.basicQos(workerConfig.getScheduledFetchLimit());

        com.rabbitmq.client.Connection updateStatusConnection = factory.newConnection("worker-update-status");
        updateStatusChannel = updateStatusConnection.createChannel();
    }

    @Scheduled(fixedDelay = 10000)
    @Observed(name = "worker.send_message.scheduled_fetch_enqueue", 
         contextualName = "worker-scheduled-fetch-enqueue",
         lowCardinalityKeyValues = {"span.kind", "internal"})
    public void scheduledFetchAndEnqueue() {
        try {
            List<Integer> campaignIds = fetchScheduledCampaigns(workerConfig.getScheduledFetchLimit());
            for (Integer campaignId : campaignIds) {
                markCampaignAsSending(campaignId);
                markCampaignLoggersAsSending(campaignId);
                updateLoggerStatus(campaignId);

                ArrayNode messages = fetchMessagesForCampaign(campaignId);
                if (messages.size() > 0) {
                    if (channel == null || !channel.isOpen()) {
                        LoggerUtil.warn("RabbitMQ channel closed, attempting to reopen...");
                        initializeRabbitMQ();
                        LoggerUtil.info("Channel reopened successfully.");
                    }

                    String payload = messages.toString();
                    channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            payload.getBytes(StandardCharsets.UTF_8));
                    LoggerUtil.info("Queued campaign " + campaignId + " with " + messages.size() + " messages");
                }
            }
        } catch (Exception e) {
            LoggerUtil.error("Error fetching and queuing scheduled campaigns", e);
        }
    }

    @Observed(name = "worker.send_message.process_message", 
         contextualName = "worker-process-message",
         lowCardinalityKeyValues = {"span.kind", "consumer"})
    private void processMessage(Delivery delivery) throws Exception {
        boolean allowSecond = FileRateLimiter.allow("yyyyMMdd_HHmmss", rateLimitConfig.getRateLimitPerSecond());
        boolean allowMinute = FileRateLimiter.allow("yyyyMMdd_HHmm", rateLimitConfig.getRateLimitPerMinute());

        if (!allowSecond || !allowMinute) {
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
            LoggerUtil.debug("Rate limit exceeded - message requeued");
            return;
        }

        if (LoggerUtil.isDebugEnabled()) {
            LoggerUtil.debug("Starting processing message");
        }

        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
        ArrayNode data;
        try {
            data = MessageValidator.validateAndStandardize(messageBody, objectMapper);
        } catch (JsonProcessingException e) {
            LoggerUtil.error("Failed to parse message as JSON. Message body: " +
                    messageBody.substring(0, Math.min(messageBody.length(), 100)) + "...", e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            errorCounter.incrementAndGet();
            return;
        }

        if (data.size() == 0) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            LoggerUtil.debug("Empty message array, acknowledged without processing");
            return;
        }

        int campaignId;
        try {
            ObjectNode firstItem = (ObjectNode) data.get(0);
            campaignId = MessageValidator.extractCampaignId(firstItem);
        } catch (Exception e) {
            LoggerUtil.error("Failed to extract campaign ID from message", e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            errorCounter.incrementAndGet();
            return;
        }

        if (!isCampaignActive(campaignId)) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            inactiveCampaignCounter.incrementAndGet();
            long now = System.currentTimeMillis();
            if (inactiveCampaignCounter.get() % 500 == 0 || (now - lastInactiveLogTime) > 10000) {
                LoggerUtil.debug("Skipped " + inactiveCampaignCounter.get() + " inactive campaigns so far.");
                lastInactiveLogTime = now;
            }
            return;
        }

        String currentTimestamp = getCurrentTimestamp();
        long startTime = System.nanoTime();

        // Process all message items using the new processMessages method
        Map<Integer, CompletableFuture<ObjectNode>> futures = processMessages(data);
        CompletableFuture<Void> allRequests = CompletableFuture
                .allOf(futures.values().toArray(new CompletableFuture[0]));
        allRequests.get();

        Map<Integer, ObjectNode> responses = new HashMap<>();
        for (Map.Entry<Integer, CompletableFuture<ObjectNode>> entry : futures.entrySet()) {
            try {
                responses.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                responses.put(entry.getKey(), createErrorResponse(e.getMessage()));
                LoggerUtil.error("Error getting response for index " + entry.getKey(), e);
            }
        }

        // Send update status messages for each item
        for (int i = 0; i < data.size(); i++) {
            if (!responses.containsKey(i)) {
                continue;
            }
            try {
                ObjectNode item = (ObjectNode) data.get(i);
                ObjectNode response = responses.get(i);
                ObjectNode updateStatusMessage = objectMapper.createObjectNode();

                // Extract data for status update; use 'data' if present or the entire item
                ObjectNode dataObj = item.has("data") ? (ObjectNode) item.get("data") : item;

                updateStatusMessage.put("messenger_bot_broadcast_serial",
                        MessageValidator.extractCampaignId(item));

                try {
                    updateStatusMessage.put("messenger_bot_broadcast_serial_send",
                            dataObj.get("messenger_bot_broadcast_serial_send").asInt());
                } catch (Exception e) {
                    updateStatusMessage.put("messenger_bot_broadcast_serial_send", 0);
                }

                try {
                    updateStatusMessage.put("messenger_bot_subscriber",
                            dataObj.get("messenger_bot_subscriber").asInt());
                } catch (Exception e) {
                    updateStatusMessage.put("messenger_bot_subscriber", 0);
                }

                updateStatusMessage.put("sent_time", getCurrentTimestamp());
                updateStatusMessage.set("response", response);

                sendUpdateStatusMessage(updateStatusMessage);
            } catch (Exception e) {
                LoggerUtil.error("Error sending update for message at index " + i, e);
            }
        }

        // Update campaign status after processing all messages
        try {
            updateCampaignStatus(campaignId, currentTimestamp);
        } catch (SQLException e) {
            LoggerUtil.error("Failed to update campaign status", e);
        }

        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        processedMessageCounter.incrementAndGet();

        if (LoggerUtil.isDebugEnabled()) {
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            LoggerUtil.debug("Completed processing " + data.size() + " messages in " + duration + "ms");
        }

        logPeriodicStats();
    }

    private CompletableFuture<ObjectNode> sendHttpRequestAsync(String url, String requestBody, String facebookToken) {
        // If a Facebook token is provided, delegate to FacebookService.
        if (facebookToken != null && !facebookToken.isEmpty()) {
            return facebookService.sendMessage(facebookToken, requestBody);
        } else {
            CompletableFuture<ObjectNode> future = new CompletableFuture<>();
            try {
                HttpPost httpPost = new HttpPost(url);
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setEntity(new StringEntity(requestBody));
                httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse result) {
                        try {
                            HttpEntity entity = result.getEntity();
                            String responseBody = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                            future.complete((ObjectNode) objectMapper.readTree(responseBody));
                        } catch (Exception e) {
                            future.complete(createErrorResponse(e.getMessage()));
                            LoggerUtil.error("Error processing HTTP response", e);
                        }
                    }

                    @Override
                    public void failed(Exception ex) {
                        future.complete(createErrorResponse(ex.getMessage()));
                        LoggerUtil.error("HTTP request failed", ex);
                    }

                    @Override
                    public void cancelled() {
                        future.complete(createErrorResponse("Request cancelled"));
                        LoggerUtil.debug("HTTP request cancelled");
                    }
                });
            } catch (Exception e) {
                future.complete(createErrorResponse(e.getMessage()));
                LoggerUtil.error("Error sending HTTP request", e);
            }
            return future;
        }
    }

    private ObjectNode createErrorResponse(String errorMessage) {
        ObjectNode response = objectMapper.createObjectNode();
        response.put("error", true);
        response.put("message", errorMessage);
        return response;
    }

    private void sendUpdateStatusMessage(ObjectNode message) throws IOException {
        updateStatusChannel.basicPublish("", updateStatusQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.toString().getBytes(StandardCharsets.UTF_8));

        if (LoggerUtil.isDebugEnabled()) {
            LoggerUtil.debug("Sent update status message for broadcast serial: " +
                    message.get("messenger_bot_broadcast_serial").asInt());
        }
    }

    private boolean isCampaignActive(int campaignId) throws SQLException {
        String query = "SELECT posting_status FROM messenger_bot_broadcast_serial WHERE id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int status = rs.getInt("posting_status");
                    return status != 4; // 3 indicates campaign is inactive/completed
                }
                return false; // Campaign not found
            }
        }
    }

    // Log statistics at intervals
    private void logPeriodicStats() {
        long now = System.currentTimeMillis();
        if (now - lastStatsLogTime > STATS_LOG_INTERVAL_MS) {
            LoggerUtil.info(String.format(
                    "Status [%s]: Processed: %d, Inactive campaigns: %d, Errors: %d",
                    Thread.currentThread().getName(),
                    processedMessageCounter.get(),
                    inactiveCampaignCounter.get(),
                    errorCounter.get()));
            lastStatsLogTime = now;
        }
    }

    // Helper method to determine if an error should cause requeuing
    private boolean shouldRequeueError(Exception e) {
        // Requeue transient errors like connection issues
        if (e instanceof java.net.ConnectException ||
                e instanceof java.net.SocketTimeoutException ||
                e instanceof java.sql.SQLTransientConnectionException) {
            return true;
        }

        // Don't requeue permanent errors like JSON parsing issues
        if (e instanceof com.fasterxml.jackson.core.JsonProcessingException) {
            return false;
        }

        // Default to not requeuing to avoid infinite loops
        return false;
    }

    // Optional method to dynamically update rate limits
    public void updateRateLimits(int newSecondsLimit, int newMinutesLimit) {
        rateLimiter.updateRateLimits(newSecondsLimit, newMinutesLimit);
        LoggerUtil.info(String.format(
                "Rate limits dynamically updated: %d messages/second, %d messages/minute",
                newSecondsLimit,
                newMinutesLimit));
    }

    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private void createDirectoryIfNotExists(String path) {
        File dir = new File(path);
        if (!dir.exists())
            dir.mkdirs();
    }

    public static void main(String[] args) {
        try {
            WorkerSendMessage worker = new WorkerSendMessage();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (worker.httpClient != null)
                        worker.httpClient.close();
                    if (worker.dbConnection != null)
                        worker.dbConnection.close();
                } catch (Exception e) {
                    LoggerUtil.error("Error shutting down", e);
                }
            }));
            worker.start();
            Thread.currentThread().join();
        } catch (Exception e) {
            LoggerUtil.error("Error starting worker", e);
            System.exit(1);
        }
    }

    private void updateCampaignStatus(int campaignId, String sentTime) throws SQLException {
        try {
            CampaignMetrics metrics = getCampaignMetrics(campaignId);

            if (metrics.successfullySent == metrics.totalThread) {
                updateCampaignStatusInDatabase(campaignId, CAMPAIGN_STATUS_COMPLETED, sentTime);
                LoggerUtil.info("Campaign " + campaignId + " completed successfully");
            } else if (metrics.lastTryErrorCount >= (metrics.totalThread * ERROR_THRESHOLD)) {
                updateCampaignStatusInDatabase(campaignId, CAMPAIGN_STATUS_ERROR, sentTime);
                LoggerUtil.warn("Campaign " + campaignId + " failed due to high error rate");
            }
        } catch (SQLException e) {
            LoggerUtil.error("Failed to update campaign status for ID " + campaignId, e);
            throw e;
        }
    }
    @Observed(name = "worker.send_message.get_campaign_metrics", 
         contextualName = "worker-get-campaign-metrics",
         lowCardinalityKeyValues = {"span.kind", "internal"})
    private CampaignMetrics getCampaignMetrics(int campaignId) throws SQLException {
        String query = """
                SELECT
                    (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ?) AS total_thread,
                    (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ? AND processed = 1) AS successfully_sent,
                    (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ? AND processed = 1 AND delivered = 0) AS last_try_error_count
                """;

        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            for (int i = 1; i <= 3; i++) {
                stmt.setInt(i, campaignId);
            }

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return new CampaignMetrics(
                        rs.getInt("total_thread"),
                        rs.getInt("successfully_sent"),
                        rs.getInt("last_try_error_count"));
            }
            throw new SQLException("No metrics found for campaign " + campaignId);
        }
    }

    private Map<Integer, CompletableFuture<ObjectNode>> processMessages(ArrayNode data) throws Exception {
        Map<Integer, CompletableFuture<ObjectNode>> futures = new HashMap<>();
        for (int i = 0; i < data.size(); i++) {
            try {
                ObjectNode item = (ObjectNode) data.get(i);
                // Normalize the request structure
                item = MessageValidator.normalizeRequestStructure(item, objectMapper);
                ObjectNode requestObj = (ObjectNode) item.get("request");
                String url = requestObj.get("url").asText().replace("\\/", "/");
                String requestBody = requestObj.get("data").toString();
                // Retrieve the Facebook token if available
                String facebookToken = "";
                if (item.has("token")) {
                    facebookToken = item.get("token").asText();
                }
                // Call the updated sendHttpRequestAsync with the token parameter.
                futures.put(i, sendHttpRequestAsync(url, requestBody, facebookToken));
            } catch (Exception e) {
                LoggerUtil.warn("Skipping malformed message at index " + i + ": " + e.getMessage());
            }
        }
        return futures;
    }

    private void updateCampaignStatusInDatabase(int campaignId, int status, String sentTime) throws SQLException {
        String updateQuery = """
                UPDATE messenger_bot_broadcast_serial
                SET posting_status = ?,
                    completed_at = ?,
                    is_try_again = 0,
                    successfully_sent = (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ? AND processed = 1),
                    successfully_delivered = (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ? AND delivered = 1),
                    last_try_error_count = (SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ? AND processed = 1 AND delivered = 0)
                WHERE id = ?
                """;

        try (PreparedStatement stmt = dbConnection.prepareStatement(updateQuery)) {
            stmt.setInt(1, status);
            stmt.setString(2, sentTime);
            for (int i = 3; i <= 6; i++) {
                stmt.setInt(i, campaignId);
            }

            int updatedRows = stmt.executeUpdate();
            if (updatedRows == 0) {
                LoggerUtil.warn("No rows updated for campaign " + campaignId);
            }
        }
    }

    // Helper class for campaign metrics
    private static class CampaignMetrics {
        final int totalThread;
        final int successfullySent;
        final int lastTryErrorCount;

        CampaignMetrics(int totalThread, int successfullySent, int lastTryErrorCount) {
            this.totalThread = totalThread;
            this.successfullySent = successfullySent;
            this.lastTryErrorCount = lastTryErrorCount;
        }
    }

    private List<Integer> fetchScheduledCampaigns(int limit) throws SQLException {
        List<Integer> campaignIds = new ArrayList<>();
        String query = """
                SELECT id FROM messenger_bot_broadcast_serial
                WHERE posting_status = 0
                AND schedule_time <= NOW()
                LIMIT ?
                """;

        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, limit);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    campaignIds.add(rs.getInt("id"));
                }
            }
        }
        return campaignIds;
    }

    private void markCampaignAsSending(int campaignId) throws SQLException {
        String query = "UPDATE messenger_bot_broadcast_serial SET posting_status = 1 WHERE id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            stmt.executeUpdate();
        }
    }

    private void markCampaignLoggersAsSending(int campaignId) throws SQLException {
        String query = "UPDATE messenger_bot_broadcast_serial_send SET processed = 0, delivered = 0 WHERE campaign_id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            stmt.executeUpdate();
        }
    }

    private void updateLoggerStatus(int campaignId) throws SQLException {
        String query = """
                UPDATE messenger_bot_broadcast_serial_send
                SET processed = 1, delivered = 1
                WHERE campaign_id = ? AND processed = 0
                """;
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            stmt.executeUpdate();
        }
    }

    private ArrayNode fetchMessagesForCampaign(int campaignId) throws SQLException {
        ArrayNode messages = objectMapper.createArrayNode();
        String query = """
                SELECT mbs.*, mbss.id as send_id, fb.page_access_token as token
                FROM messenger_bot_broadcast_serial mbs
                JOIN messenger_bot_broadcast_serial_send mbss ON mbs.id = mbss.campaign_id
                JOIN facebook_rx_fb_page_info fb ON fb.id = mbss.page_id
                WHERE mbs.id = ? AND mbss.processed = 0
                """;
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ObjectNode message = objectMapper.createObjectNode();
                    message.put("campaign_id", rs.getInt("id"));
                    message.put("send_id", rs.getInt("send_id"));
                    message.put("message", rs.getString("message"));
                    // Inclui o token do Facebook
                    message.put("token", rs.getString("token"));
                    messages.add(message);
                }
            }
        }
        return messages;
    }

    @Observed(name = "worker.send_message.start", 
         contextualName = "worker-send-message-start",
         lowCardinalityKeyValues = {"span.kind", "internal"})
    public synchronized void start() throws Exception {
        createDirectoryIfNotExists("storage/logs");
        createDirectoryIfNotExists("storage/rate-limit");

        // Initialize RabbitMQ queues
        channel.queueDeclare(queueName, true, false, false, null);
        updateStatusChannel.queueDeclare(updateStatusQueueName, true, false, false, null);

        // Set up message consumer
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                processMessage(delivery);
            } catch (Exception e) {
                LoggerUtil.error("Error processing message", e);
                boolean shouldRequeue = shouldRequeueError(e);
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), shouldRequeue);
            }
        };

        CancelCallback cancelCallback = consumerTag -> LoggerUtil.warn("Consumer cancelled by broker: " + consumerTag);

        // Start consuming messages
        channel.basicConsume(queueName, false, deliverCallback, cancelCallback);
        LoggerUtil.info("Started consuming messages from queue: " + queueName);
    }
}