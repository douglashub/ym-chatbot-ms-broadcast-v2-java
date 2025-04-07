package com.ymchatbot.worker;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.ymchatbot.config.DatabaseConfig;
import com.ymchatbot.config.RabbitMQConfig;
import com.ymchatbot.config.WorkerConfig;
import com.ymchatbot.util.LoggerUtil;

import io.micrometer.observation.annotation.Observed;

@Component
public class WorkerUpdateStatus {
    private final String queueName = "broadcast-v2/update-status";
    private static final int MAX_DB_RECONNECT_ATTEMPTS = 3;

    @Autowired
    private DatabaseConfig databaseConfig;

    @Autowired
    private RabbitMQConfig rabbitMQConfig;

    @Autowired
    private WorkerConfig workerConfig;

    // State tracking
    private volatile boolean initialized = false;
    private volatile boolean started = false;

    private java.sql.Connection dbConnection;
    private Channel channel;
    private com.rabbitmq.client.Connection connection;

    private static AtomicLong processedCount = new AtomicLong(0);
    private static AtomicLong successCount = new AtomicLong(0);
    private static AtomicLong errorCount = new AtomicLong(0);
    private static long lastStatsLogTime = 0;
    private static final long STATS_LOG_INTERVAL_MS = 10000;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void initFromSpring() throws Exception {
        if (!initialized) {
            initialize();
        }
        if (!started) {
            start();
        }
    }

    @Observed(name = "worker.update_status.initialize",
              contextualName = "worker-update-status-initialize")
    public synchronized void initialize() throws Exception {
        if (initialized) {
            LoggerUtil.info("WorkerUpdateStatus already initialized, skipping initialization");
            return;
        }

        LoggerUtil.info("⏳ Initializing WorkerUpdateStatus...");

        try {
            initDbConnection();
            initializeRabbitMQ();
            initialized = true;
            LoggerUtil.info("✅ WorkerUpdateStatus initialized successfully");
        } catch (Exception e) {
            LoggerUtil.error("❌ Failed to initialize WorkerUpdateStatus", e);
            throw e;
        }
    }

    @Observed(name = "worker.update_status.start",
              contextualName = "worker-update-status-start")
    public synchronized void start() throws IOException {
        if (started) {
            LoggerUtil.info("WorkerUpdateStatus already started, skipping start");
            return;
        }

        if (!initialized) {
            LoggerUtil.error("Cannot start WorkerUpdateStatus: not initialized");
            throw new IllegalStateException("WorkerUpdateStatus not initialized");
        }

        LoggerUtil.info("🚀 Starting WorkerUpdateStatus...");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                LoggerUtil.info("📩 Received message from update-status queue: " +
                        messageBody.substring(0, Math.min(100, messageBody.length())) + "...");

                boolean processed = processMessage(delivery);

                try {
                    if (channel == null || !channel.isOpen()) {
                        LoggerUtil.warn("⚠️ Channel closed before acknowledging message, reconnecting...");
                        reconnectChannel();
                    }

                    if (processed) {
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } else {
                        channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                    }
                } catch (IOException ioe) {
                    LoggerUtil.error("❌ Failed to acknowledge/reject message after processing", ioe);
                    reconnectChannel();
                }
            } catch (Exception e) {
                LoggerUtil.error("❌ Error processing message: " + e.getMessage(), e);
                handleMessageError(delivery, e);
            }
        };

        channel.basicQos(5);

        for (int i = 0; i < workerConfig.getUpdateStatusConcurrentConsumers(); i++) {
            String consumerTag = channel.basicConsume(queueName, false, deliverCallback, tag -> {
                LoggerUtil.info("Consumer cancelled: " + tag);
            });
            LoggerUtil.info("👤 Started consumer #" + (i + 1) + " with tag: " + consumerTag);
        }

        started = true;
        LoggerUtil.info("🚀 Update Status worker started with " + workerConfig.getUpdateStatusConcurrentConsumers()
                + " consumers");
    }

    private void handleMessageError(Delivery delivery, Exception e) {
        try {
            if (e instanceof IOException || channel == null || !channel.isOpen()) {
                reconnectChannel();
            }
            boolean requeue = shouldRequeueError(e);
            channel.basicReject(delivery.getEnvelope().getDeliveryTag(), requeue);
        } catch (Exception ex) {
            LoggerUtil.error("❌ Failed to reject message after error", ex);
        }
    }

    private void reconnectChannel() {
        try {
            LoggerUtil.info("🔄 Attempting to reconnect RabbitMQ channel...");
            if (connection == null || !connection.isOpen()) {
                connection = rabbitMQConfig.rabbitConnectionFactory().newConnection("worker-update-status");
            }

            if (channel == null || !channel.isOpen()) {
                channel = connection.createChannel();
                channel.queueDeclare(queueName, true, false, false, null);
            }
        } catch (Exception e) {
            LoggerUtil.error("❌ Failed to reconnect RabbitMQ channel", e);
        }
    }

    private void logPeriodicStats() {
        long now = System.currentTimeMillis();
        if (now - lastStatsLogTime > STATS_LOG_INTERVAL_MS) {
            LoggerUtil.info(String.format(
                    "📊 Status Update Worker [%s]: Processed: %d, Success: %d, Errors: %d",
                    Thread.currentThread().getName(),
                    processedCount.get(), successCount.get(), errorCount.get()));
            lastStatsLogTime = now;
        }
    }

    private boolean shouldRequeueError(Exception e) {
        // Requeue for transient errors como conexão e timeout
        if (e instanceof SQLException) {
            String message = e.getMessage().toLowerCase();
            // Requeue para erros de conexão, mas não para outros erros SQL
            boolean shouldRequeue = message.contains("connection") ||
                    message.contains("timeout") ||
                    message.contains("deadlock");
            LoggerUtil.debug("SQL error will " + (shouldRequeue ? "" : "not ") + "be requeued: " + e.getMessage());
            return shouldRequeue;
        }

        if (e instanceof IOException) {
            LoggerUtil.debug("IOException will be requeued: " + e.getMessage());
            return true;
        }

        // Não requeue para erros de formato de mensagem
        if (e instanceof JsonProcessingException) {
            LoggerUtil.debug("JsonProcessingException will not be requeued: " + e.getMessage());
            return false;
        }

        // Por padrão, requeue erros desconhecidos
        LoggerUtil.debug("Unknown error type will be requeued: " + e.getClass().getName());
        return true;
    }

    private void initDbConnection() throws SQLException {
        int attempts = 0;
        SQLException lastException = null;

        while (attempts < MAX_DB_RECONNECT_ATTEMPTS) {
            try {
                LoggerUtil.info(
                        "Connecting to database (attempt " + (attempts + 1) + "/" + MAX_DB_RECONNECT_ATTEMPTS + ")...");
                dbConnection = databaseConfig.dataSource().getConnection();
                LoggerUtil.info("✅ Database connection established");
                return;
            } catch (SQLException e) {
                lastException = e;
                attempts++;
                LoggerUtil.error(
                        "❌ Failed to connect to database (attempt " + attempts + "/" + MAX_DB_RECONNECT_ATTEMPTS + ")",
                        e);

                if (attempts < MAX_DB_RECONNECT_ATTEMPTS) {
                    try {
                        Thread.sleep(1000 * attempts); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Database connection interrupted", ie);
                    }
                }
            }
        }

        LoggerUtil.error("❌ Failed to connect to database after " + MAX_DB_RECONNECT_ATTEMPTS + " attempts");
        throw lastException;
    }

    private void initializeRabbitMQ() throws Exception {
        ConnectionFactory factory = rabbitMQConfig.rabbitConnectionFactory();
        connection = factory.newConnection("worker-update-status");
        channel = connection.createChannel();

        // Declare queue with durable flag
        boolean durable = true;
        channel.queueDeclare(queueName, durable, false, false, null);
        channel.basicQos(workerConfig.getUpdateStatusConcurrentConsumers());
    }

    private boolean processMessage(Delivery delivery) throws Exception {
        long count = processedCount.incrementAndGet();
        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);

        LoggerUtil.debug("📄 Full message content: " + messageBody);
        LoggerUtil.info("📩 Processing message #" + count);

        try {
            JsonNode message = objectMapper.readTree(messageBody);

            // Extract campaign and message IDs
            int campaignId = message.path("messenger_bot_broadcast_serial").asInt();
            int messageId = message.path("messenger_bot_broadcast_serial_send").asInt();

            if (campaignId == 0 || messageId == 0) {
                LoggerUtil.error("❌ Invalid message format: missing required fields");
                errorCount.incrementAndGet();
                return false;
            }

            // Check for error response
            JsonNode response = message.path("response");
            if (response.has("error")) {
                JsonNode error = response.get("error");
                String errorMessage = error.path("message").asText("Unknown error");
                int errorCode = error.path("code").asInt(0);

                // Update message status and subscriber if needed
                boolean success = updateMessageWithError(campaignId, messageId, error);
                if (errorCode == 551) {
                    // Update subscriber status for permanent errors
                    updateSubscriberStatus(campaignId, messageId, errorMessage);
                }
                errorCount.incrementAndGet();
                return success;
            } else {
                // Handle success case
                String externalMsgId = response.path("message_id").asText("");
                boolean success = updateMessageWithSuccess(campaignId, messageId, externalMsgId);
                if (success) {
                    successCount.incrementAndGet();
                }
                return success;
            }
        } catch (Exception e) {
            errorCount.incrementAndGet();
            LoggerUtil.error("❌ Error processing message: " + e.getMessage(), e);
            throw e;
        } finally {
            logPeriodicStats();
        }
    }

    private boolean updateMessageWithSuccess(int campaignId, int messageId, String externalMessageId)
            throws SQLException {
        LoggerUtil.info("🔄 Updating message as success - campaignId: " + campaignId + ", messageId: " + messageId);

        if (!ensureDbConnection()) {
            LoggerUtil.error("❌ Cannot update message - Database connection failed");
            return false;
        }

        boolean originalAutoCommit = false;
        try {
            originalAutoCommit = dbConnection.getAutoCommit();
            dbConnection.setAutoCommit(false);
            LoggerUtil.debug("🔄 Set autoCommit=false for transaction");

            String sql = "UPDATE messenger_bot_broadcast_serial_send " +
                    "SET processed = '1', delivered = '1', message_sent_id = ?, processed_by = 'java-ms-v2' " +
                    "WHERE campaign_id = ? AND id = ?";

            LoggerUtil.debug("🔄 Executing SQL: " + sql + " with params: [" + externalMessageId + ", " + campaignId
                    + ", " + messageId + "]");

            try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
                stmt.setString(1, externalMessageId);
                stmt.setInt(2, campaignId);
                stmt.setInt(3, messageId);
                int updated = stmt.executeUpdate();

                if (updated == 0) {
                    LoggerUtil.warn("⚠️ No rows updated for success message - campaignId: " + campaignId
                            + ", messageId: " + messageId);
                    return false;
                } else {
                    LoggerUtil.info(
                            "✅ Successfully updated message - campaignId: " + campaignId + ", messageId: " + messageId);
                }
            }

            dbConnection.commit();
            LoggerUtil.debug("✅ Transaction committed successfully");
            return true;
        } catch (SQLException e) {
            LoggerUtil.error("❌ SQL error in updateMessageWithSuccess: " + e.getMessage(), e);
            try {
                LoggerUtil.debug("🔄 Rolling back transaction due to error");
                dbConnection.rollback();
            } catch (SQLException re) {
                LoggerUtil.error("Failed to rollback transaction", re);
            }
            throw e;
        } finally {
            try {
                LoggerUtil.debug("🔄 Restoring original autoCommit=" + originalAutoCommit);
                dbConnection.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                LoggerUtil.error("Failed to restore autoCommit setting", e);
            }
        }
    }

    private boolean ensureDbConnection() {
        for (int attempt = 1; attempt <= MAX_DB_RECONNECT_ATTEMPTS; attempt++) {
            try {
                if (dbConnection == null || dbConnection.isClosed()) {
                    LoggerUtil.info("🔄 Reconnecting to database (attempt " + attempt + ")");
                    initDbConnection();
                }

                // Testar a conexão com uma consulta simples
                try (Statement stmt = dbConnection.createStatement()) {
                    stmt.executeQuery("SELECT 1");
                    return true;
                }
            } catch (SQLException e) {
                LoggerUtil.error("❌ Failed to ensure database connection on attempt " + attempt + ": " + e.getMessage(),
                        e);
                if (attempt < MAX_DB_RECONNECT_ATTEMPTS) {
                    try {
                        int sleepTime = 1000 * attempt; // Backoff exponencial
                        LoggerUtil.info("Waiting " + sleepTime + "ms before retry...");
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LoggerUtil.warn("Thread interrupted while waiting to retry database connection");
                    }
                }
            }
        }
        LoggerUtil.error("❌ Failed to establish database connection after " + MAX_DB_RECONNECT_ATTEMPTS + " attempts");
        return false;
    }

    private boolean updateMessageWithError(int campaignId, int messageId, JsonNode error) throws SQLException {
        LoggerUtil.info("🔄 Updating message as error - campaignId: " + campaignId + ", messageId: " + messageId);

        if (!ensureDbConnection()) {
            LoggerUtil.error("❌ Cannot update message - Database connection failed");
            return false;
        }

        boolean originalAutoCommit = false;
        try {
            originalAutoCommit = dbConnection.getAutoCommit();
            dbConnection.setAutoCommit(false);
            LoggerUtil.debug("🔄 Set autoCommit=false for transaction");

            String errorMessage = error.path("message").asText("Unknown error");
            int errorCode = error.path("code").asInt(0);

            String errorText = "Code: " + errorCode + ", Message: " + errorMessage;
            LoggerUtil.info("🔍 Error details: " + errorText);

            String updateSql = "UPDATE messenger_bot_broadcast_serial_send " +
                    "SET processed = '1', delivered = '0', error_message = ?, processed_by = 'java-ms-v2' " +
                    "WHERE campaign_id = ? AND id = ?";

            LoggerUtil.debug("🔄 Executing SQL: " + updateSql + " with params: [" + errorText + ", " + campaignId + ", "
                    + messageId + "]");

            try (PreparedStatement stmt = dbConnection.prepareStatement(updateSql)) {
                stmt.setString(1, errorText);
                stmt.setInt(2, campaignId);
                stmt.setInt(3, messageId);
                int updated = stmt.executeUpdate();

                if (updated == 0) {
                    LoggerUtil.warn("⚠️ No rows updated for error message - campaignId: " + campaignId + ", messageId: "
                            + messageId);
                    return false;
                } else {
                    LoggerUtil.info("✅ Successfully updated error message - campaignId: " + campaignId
                            + ", messageId: " + messageId);
                }
            }

            // Atualizar subscriber se necessário (código 551 = usuário indisponível)
            if (errorCode == 551) {
                try {
                    updateSubscriberStatus(campaignId, messageId, errorMessage);
                } catch (SQLException e) {
                    LoggerUtil.error("❌ Error updating subscriber status", e);
                    // Não re-throw para não afetar a transação principal
                }
            }

            dbConnection.commit();
            LoggerUtil.debug("✅ Transaction committed successfully");
            return true;
        } catch (SQLException e) {
            LoggerUtil.error("❌ SQL error in updateMessageWithError: " + e.getMessage(), e);
            try {
                LoggerUtil.debug("🔄 Rolling back transaction due to error");
                dbConnection.rollback();
            } catch (SQLException re) {
                LoggerUtil.error("Failed to rollback transaction", re);
            }
            throw e;
        } finally {
            try {
                LoggerUtil.debug("🔄 Restoring original autoCommit=" + originalAutoCommit);
                dbConnection.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                LoggerUtil.error("Failed to restore autoCommit setting", e);
            }
        }
    }

    private void updateSubscriberStatus(int campaignId, int messageId, String errorMessage) throws SQLException {
        LoggerUtil.debug("🔄 Updating subscriber status for campaign=" + campaignId + ", message=" + messageId);

        String sql = """
                UPDATE messenger_bot_subscriber
                SET last_error_message = ?, unavailable = 1
                WHERE id = (
                    SELECT messenger_bot_subscriber
                    FROM messenger_bot_broadcast_serial_send
                    WHERE campaign_id = ? AND id = ?
                    LIMIT 1
                )""";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, errorMessage);
            stmt.setInt(2, campaignId);
            stmt.setInt(3, messageId);
            int updated = stmt.executeUpdate();
            LoggerUtil.debug("✅ Updated " + updated + " subscriber records as unavailable");
        }
    }

    /**
     * Returns the current started state of the worker
     * 
     * @return boolean indicating if the worker has been started
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Returns the current initialization state of the worker
     * 
     * @return boolean indicating if the worker has been initialized
     */
    public boolean isInitialized() {
        return initialized;
    }
}