package com.ymchatbot;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import jakarta.annotation.PostConstruct;
import com.rabbitmq.client.*;
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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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

@Component
@EnableScheduling
public class WorkerSendMessage {
    private final String queueName = "broadcast-v2/send-messages";
    private final String updateStatusQueueName = "broadcast-v2/update-status";
    private final int MAX_CONNECTIONS = 200;

    // Add campaign status constants
    private static final int CAMPAIGN_STATUS_COMPLETED = 2;
    private static final int CAMPAIGN_STATUS_ERROR = 4;
    private static final double ERROR_THRESHOLD = 0.5;

    @Value("${application.rate-limit.script.seconds:10000}")
    private int rateLimitPerSecond;
    
    @Value("${application.rate-limit.script.minutes:600000}")
    private int rateLimitPerMinute;

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${spring.rabbitmq.host}")
    private String amqpHost;

    @Value("${spring.rabbitmq.port}")
    private int amqpPort;

    @Value("${spring.rabbitmq.username}")
    private String amqpUsername;

    @Value("${spring.rabbitmq.password}")
    private String amqpPassword;

    @Value("${spring.rabbitmq.virtual-host:/}")
    private String amqpVhost;

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
        // Initialize RateLimiter
        rateLimiter = new RateLimiter(MAX_CONNECTIONS, MAX_CONNECTIONS);

        LoggerUtil.info("Starting WorkerSendMessage with rate limits: " + rateLimitPerSecond + "s / "
                + rateLimitPerMinute + "m");

        initDbConnection();
        initHttpClient();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(amqpHost);
        factory.setPort(amqpPort);
        factory.setUsername(amqpUsername);
        factory.setPassword(amqpPassword);
        factory.setVirtualHost(amqpVhost);

        com.rabbitmq.client.Connection connection = factory.newConnection();
        channel = connection.createChannel();
        channel.basicQos(50);

        com.rabbitmq.client.Connection updateStatusConnection = factory.newConnection();
        updateStatusChannel = updateStatusConnection.createChannel();

        LoggerUtil.info("WorkerSendMessage initialization complete");
    }

    private void initDbConnection() throws SQLException {
        dbConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        LoggerUtil.info("Database connection established to " + dbUrl);
    }

    private void initHttpClient() throws Exception {
        try {
            PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(
                    new DefaultConnectingIOReactor());
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

    public void start() throws IOException {
        createDirectoryIfNotExists("../storage/logs");
        createDirectoryIfNotExists("../storage/rate-limit");

        // Declare queues with durable flag
        boolean durable = true;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String, Object> arguments = null;

        // Declare queues using retry mechanism
        declareQueueWithRetry(channel, queueName, durable, exclusive, autoDelete, arguments);
        declareQueueWithRetry(updateStatusChannel, updateStatusQueueName, durable, exclusive, autoDelete, arguments);

        // Set up consumer
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                processMessage(delivery);
            } catch (Exception e) {
                errorCounter.incrementAndGet();
                LoggerUtil.error("Error processing message", e);
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), shouldRequeueError(e));
            }
        };

        LoggerUtil.info("Starting to consume from queue: " + queueName);
        channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
            LoggerUtil.debug("Consumer cancelled: " + consumerTag);
        });

        LoggerUtil.info("Worker started and waiting for messages from queue: " + queueName);
    }

    private void markCampaignAsSending(int campaignId) throws SQLException {
        String update = "UPDATE messenger_bot_broadcast_serial SET posting_status = 1, is_try_again = 0 WHERE id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(update)) {
            stmt.setInt(1, campaignId);
            stmt.executeUpdate();
        }
    }

    private void updateLoggerStatus(int campaignId) {
        try {
            String loggerQuery = "INSERT INTO messenger_bot_broadcast_serial_logger (status) VALUES (5)";
            String loggerSerialQuery = "INSERT INTO messenger_bot_broadcast_serial_logger_serial (logger_id, serial_id) " +
                                       "VALUES (LAST_INSERT_ID(), ?)";
    
            try (PreparedStatement loggerStmt = dbConnection.prepareStatement(loggerQuery, Statement.RETURN_GENERATED_KEYS);
                 PreparedStatement loggerSerialStmt = dbConnection.prepareStatement(loggerSerialQuery)) {
    
                loggerStmt.executeUpdate();
                
                try (ResultSet generatedKeys = loggerStmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        loggerSerialStmt.setInt(1, campaignId);
                        loggerSerialStmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            LoggerUtil.warn("Could not update logger status for campaign " + campaignId + ": " + e.getMessage());
        }
    }
    
    @Scheduled(fixedDelay = 10000)
    public void scheduledFetchAndEnqueue() {
        try {
            List<Integer> campaignIds = fetchScheduledCampaigns(10); // Adjust limit as needed
            for (Integer campaignId : campaignIds) {
                markCampaignAsSending(campaignId);
                markCampaignLoggersAsSending(campaignId); // Add this line
                
                // Update logger status to 5 (sending)
                updateLoggerStatus(campaignId);

                JSONArray messages = fetchMessagesForCampaign(campaignId);
                if (messages.length() > 0) {
                    // Check if channel is closed before publishing
                    if (channel == null || !channel.isOpen()) {
                        LoggerUtil.warn("RabbitMQ channel closed, attempting to reopen...");
                        ConnectionFactory factory = new ConnectionFactory();
                        factory.setHost(amqpHost);
                        factory.setPort(amqpPort);
                        factory.setUsername(amqpUsername);
                        factory.setPassword(amqpPassword);
                        factory.setVirtualHost(amqpVhost);
                        com.rabbitmq.client.Connection connection = factory.newConnection();
                        channel = connection.createChannel();
                        channel.basicQos(50);
                        LoggerUtil.info("Channel reopened successfully.");
                    }

                    String payload = messages.toString();
                    channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            payload.getBytes(StandardCharsets.UTF_8));
                    LoggerUtil.info("Queued campaign " + campaignId + " with " + messages.length() + " messages");
                }
            }
        } catch (Exception e) {
            LoggerUtil.error("Error fetching and queuing scheduled campaigns", e);
        }
    }

    private JSONArray fetchMessagesForCampaign(int campaignId) throws SQLException {
        String query = """
            SELECT 
                s.id AS messenger_bot_broadcast_serial_send,
                s.messenger_bot_subscriber,
                p.page_access_token AS token,
                c.message AS message,
                s.subscriber_name AS lead_name,
                s.subscriber_lastname AS lead_last_name,
                s.subscribe_id AS subscribe_id
            FROM messenger_bot_broadcast_serial_send s
            JOIN facebook_rx_fb_page_info p ON p.id = s.page_id
            JOIN messenger_bot_broadcast_serial c ON c.id = s.campaign_id
            WHERE s.campaign_id = ? AND s.processed = '0'
            LIMIT 100
        """;
    
        JSONArray result = new JSONArray();
        try (PreparedStatement stmt = dbConnection.prepareStatement(query)) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    // Prepare message with replacements
                    String message = rs.getString("message");
                    if (message == null) {
                        LoggerUtil.warn("Null message for campaign " + campaignId);
                        continue;
                    }
    
                    message = message.replace("{{first_name}}", rs.getString("lead_name") != null ? rs.getString("lead_name") : "")
                                     .replace("{{last_name}}", rs.getString("lead_last_name") != null ? rs.getString("lead_last_name") : "")
                                     .replace("PUT_OTN_TOKEN", rs.getString("token") != null ? rs.getString("token") : "")
                                     .replace("PUT_SUBSCRIBER_ID", rs.getString("subscribe_id") != null ? rs.getString("subscribe_id") : "")
                                     .replace("#SUBSCRIBER_ID_REPLACE#", rs.getString("subscribe_id") != null ? rs.getString("subscribe_id") : "");
    
                    JSONObject item = new JSONObject();
                    item.put("data", new JSONObject(){{
                        put("messenger_bot_broadcast_serial", campaignId);
                        put("messenger_bot_broadcast_serial_send", rs.getInt("messenger_bot_broadcast_serial_send"));
                        put("messenger_bot_subscriber", rs.getInt("messenger_bot_subscriber"));
                    }});
    
                    JSONObject request = new JSONObject();
                    request.put("url", "https://graph.facebook.com/v22.0/me/messages?access_token=" + rs.getString("token"));
                    request.put("data", new JSONObject(message));
    
                    item.put("request", request);
                    result.put(item);
                }
            }
        }
        return result;
    }
    
    private List<Integer> fetchScheduledCampaigns(int limit) throws SQLException {
        String query = """
                    SELECT id FROM messenger_bot_broadcast_serial
                    WHERE schedule_time < NOW()
                    AND (posting_status = 0 OR is_try_again = 1)
                    AND posting_status != 3
                    ORDER BY schedule_time ASC, total_thread ASC
                    LIMIT ?
                """;

        List<Integer> campaignIds = new ArrayList<>();
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

    private void declareQueueWithRetry(Channel channel, String queueName, boolean durable,
            boolean exclusive, boolean autoDelete,
            Map<String, Object> arguments) throws IOException {
        int maxRetries = 5;
        int retryDelayMs = 1000; // Starting with 1 second

        IOException lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                LoggerUtil.info("Attempt " + attempt + " - Declaring queue: " + queueName);

                // Declare the queue
                channel.queueDeclare(queueName, durable, exclusive, autoDelete, arguments);

                // Verify it exists (this will throw an exception if the queue doesn't exist)
                channel.queueDeclarePassive(queueName);

                // If we get here, the queue exists
                LoggerUtil.info("Queue successfully declared and verified: " + queueName);
                return;
            } catch (IOException e) {
                lastException = e;
                LoggerUtil.warn("Queue operation failed on attempt " + attempt + ": " + e.getMessage());

                if (attempt < maxRetries) {
                    try {
                        LoggerUtil.info("Waiting " + retryDelayMs + "ms before retrying...");
                        Thread.sleep(retryDelayMs);
                        // Exponential backoff
                        retryDelayMs *= 2;
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting to retry queue creation", ie);
                    }
                }
            }
        }

        // If we got here, all retries failed
        throw new IOException("Failed to declare and verify queue after " + maxRetries + " attempts", lastException);
    }

    private void markCampaignLoggersAsSending(int campaignId) {
        String sql = """
            UPDATE messenger_bot_broadcast_serial_logger 
            SET status = 5 
            WHERE status = 2 AND id IN (
                SELECT logger_id 
                FROM messenger_bot_broadcast_serial_logger_serial 
                WHERE serial_id = ?
            )
        """;

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setInt(1, campaignId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            LoggerUtil.error("Failed to update campaign loggers status", e);
        }
    }

    private void processMessage(Delivery delivery) throws Exception {
        // Add rate limiting check at start
        boolean allowSecond = FileRateLimiter.allow("yyyyMMdd_HHmmss", rateLimitPerSecond);
        boolean allowMinute = FileRateLimiter.allow("yyyyMMdd_HHmm", rateLimitPerMinute);
        
        if (!allowSecond || !allowMinute) {
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
            LoggerUtil.debug("Rate limit exceeded - message requeued");
            return;
        }
        
        // Only log at DEBUG level when really needed
        if (LoggerUtil.isDebugEnabled()) {
            LoggerUtil.debug("Starting processing message");
        }

        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);

        // Use the validator to standardize the message format
        JSONArray data;
        try {
            data = MessageValidator.validateAndStandardize(messageBody);
        } catch (JSONException e) {
            LoggerUtil.error("Failed to parse message as JSON. Message body: " +
                    messageBody.substring(0, Math.min(messageBody.length(), 100)) + "...", e);
            // Don't requeue malformed messages to avoid infinite loops
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            errorCounter.incrementAndGet();
            return;
        }

        // Empty array check
        if (data.length() == 0) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            LoggerUtil.debug("Empty message array, acknowledged without processing");
            return;
        }

        // Check if campaign is active
        int campaignId;
        try {
            JSONObject firstItem = data.getJSONObject(0);
            campaignId = MessageValidator.extractCampaignId(firstItem);
        } catch (JSONException e) {
            LoggerUtil.error("Failed to extract campaign ID from message", e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            errorCounter.incrementAndGet();
            return;
        }

        if (!isCampaignActive(campaignId)) {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

            // Batch logging for inactive campaigns
            inactiveCampaignCounter.incrementAndGet();
            long now = System.currentTimeMillis();
            if (inactiveCampaignCounter.get() % 500 == 0 || (now - lastInactiveLogTime) > 10_000) {
                LoggerUtil.debug("Skipped " + inactiveCampaignCounter.get() + " inactive campaigns so far.");
                lastInactiveLogTime = now;
            }

            return;
        }

        String currentTimestamp = getCurrentTimestamp();

        // Process messages
        long startTime = System.nanoTime();
        Map<Integer, CompletableFuture<JSONObject>> futures = new HashMap<>();

        // Process each message item
        for (int i = 0; i < data.length(); i++) {
            try {
                JSONObject item = data.getJSONObject(i);
                // Normalize the request structure
                item = MessageValidator.normalizeRequestStructure(item);

                JSONObject requestObj = item.getJSONObject("request");
                String url = requestObj.getString("url").replace("\\/", "/");
                String requestBody = requestObj.getJSONObject("data").toString();

                futures.put(i, sendHttpRequestAsync(url, requestBody));
            } catch (JSONException e) {
                LoggerUtil.warn("Skipping malformed message at index " + i + ": " + e.getMessage());
                // Continue processing other messages
            }
        }

        // Wait for all requests to complete
        CompletableFuture<Void> allRequests = CompletableFuture
                .allOf(futures.values().toArray(new CompletableFuture[0]));
        allRequests.get();

        // Process responses
        Map<Integer, JSONObject> responses = new HashMap<>();
        for (Map.Entry<Integer, CompletableFuture<JSONObject>> entry : futures.entrySet()) {
            try {
                responses.put(entry.getKey(), entry.getValue().get());
            } catch (Exception e) {
                responses.put(entry.getKey(), createErrorResponse(e.getMessage()));
                LoggerUtil.error("Error getting response for index " + entry.getKey(), e);
            }
        }

        // Send status updates
        for (int i = 0; i < data.length(); i++) {
            if (!responses.containsKey(i)) {
                continue; // Skip if this index was malformed and not processed
            }

            try {
                JSONObject item = data.getJSONObject(i);
                JSONObject response = responses.get(i);
                JSONObject updateStatusMessage = new JSONObject();

                // Extract data for status update
                JSONObject dataObj = item.has("data") ? item.getJSONObject("data") : item;

                updateStatusMessage.put("messenger_bot_broadcast_serial",
                        MessageValidator.extractCampaignId(item));

                // Extract other required fields or use defaults
                try {
                    updateStatusMessage.put("messenger_bot_broadcast_serial_send",
                            dataObj.getInt("messenger_bot_broadcast_serial_send"));
                } catch (JSONException e) {
                    updateStatusMessage.put("messenger_bot_broadcast_serial_send", 0);
                }

                try {
                    updateStatusMessage.put("messenger_bot_subscriber",
                            dataObj.getInt("messenger_bot_subscriber"));
                } catch (JSONException e) {
                    updateStatusMessage.put("messenger_bot_subscriber", 0);
                }

                updateStatusMessage.put("sent_time", getCurrentTimestamp());
                updateStatusMessage.put("response", response);

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
            // Don't rethrow to avoid message requeue
        }

        // Acknowledge the delivery
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

        // Update counters and log stats
        processedMessageCounter.incrementAndGet();

        if (LoggerUtil.isDebugEnabled()) {
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            LoggerUtil.debug("Completed processing " + data.length() + " messages in " + duration + "ms");
        }

        // Log stats periodically
        logPeriodicStats();
    }

    private CompletableFuture<JSONObject> sendHttpRequestAsync(String url, String requestBody) {
        CompletableFuture<JSONObject> future = new CompletableFuture<>();
        try {
            HttpPost httpPost = new HttpPost(url);
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setEntity(new StringEntity(requestBody));
            httpClient.execute(httpPost, new FutureCallback<HttpResponse>() {
                public void completed(org.apache.http.HttpResponse result) {
                    try {
                        HttpEntity entity = result.getEntity();
                        String responseBody = EntityUtils.toString(entity);
                        future.complete(new JSONObject(responseBody));
                    } catch (Exception e) {
                        future.complete(createErrorResponse(e.getMessage()));
                        LoggerUtil.error("Error processing HTTP response", e);
                    }
                }

                public void failed(Exception ex) {
                    future.complete(createErrorResponse(ex.getMessage()));
                    LoggerUtil.error("HTTP request failed", ex);
                }

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

    private JSONObject createErrorResponse(String message) {
        JSONObject error = new JSONObject();
        error.put("message", message);
        error.put("code", 0);
        JSONObject response = new JSONObject();
        response.put("error", error);
        return response;
    }

    private void sendUpdateStatusMessage(JSONObject message) throws IOException {
        updateStatusChannel.basicPublish("", updateStatusQueueName, MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.toString().getBytes(StandardCharsets.UTF_8));

        if (LoggerUtil.isDebugEnabled()) {
            LoggerUtil.debug("Sent update status message for broadcast serial: " +
                    message.getInt("messenger_bot_broadcast_serial"));
        }
    }

    private boolean isCampaignActive(int campaignId) throws SQLException {
        try (PreparedStatement stmt = dbConnection
                .prepareStatement("SELECT posting_status FROM messenger_bot_broadcast_serial WHERE id = ?")) {
            stmt.setInt(1, campaignId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("posting_status") != 4;
            }
        }
        return false;
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
        if (e instanceof org.json.JSONException) {
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
}
