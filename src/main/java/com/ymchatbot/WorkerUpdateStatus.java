package com.ymchatbot;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONException;
import org.json.JSONObject;

@Component
public class WorkerUpdateStatus {
    private final String queueName = "broadcast-v2/update-status";

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

    @Value("${application.worker.update-status.concurrent-consumers:2}")
    private int concurrentConsumers;

    private java.sql.Connection dbConnection;
    private Channel channel;
    private com.rabbitmq.client.Connection rabbitConnection;

    private static AtomicLong processedCount = new AtomicLong(0);
    private static AtomicLong successCount = new AtomicLong(0);
    private static AtomicLong errorCount = new AtomicLong(0);
    private static long lastStatsLogTime = 0;
    private static final long STATS_LOG_INTERVAL_MS = 30000;

    private boolean initialized = false;
    private boolean started = false;

    @PostConstruct
    public void initFromSpring() throws Exception {
        if (!initialized) initialize();
        if (!started) start();
    }

    public void initialize() throws Exception {
        if (initialized) return;

        initDbConnection();
        reconnectChannel();

        initialized = true;
        LoggerUtil.info("✅ WorkerUpdateStatus initialization complete");
    }

    private void reconnectChannel() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(amqpHost);
        factory.setPort(amqpPort);
        factory.setUsername(amqpUsername);
        factory.setPassword(amqpPassword);
        factory.setVirtualHost(amqpVhost);

        rabbitConnection = factory.newConnection();
        channel = rabbitConnection.createChannel();
        channel.basicQos(50);
        LoggerUtil.info("🔁 Channel (re)connected");
    }

    private void initDbConnection() throws SQLException {
        dbConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        LoggerUtil.info("✅ Database connection established to " + dbUrl);
    }

    public void start() throws IOException {
        if (started) return;

        boolean durable = true;
        channel.queueDeclare(queueName, durable, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                processMessage(delivery);
            } catch (Exception e) {
                LoggerUtil.error("❌ Error processing status update message", e);
                safeReject(delivery.getEnvelope().getDeliveryTag(), true);
            }
        };

        for (int i = 0; i < concurrentConsumers; i++) {
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
                LoggerUtil.debug("⚠️ Consumer cancelled: " + consumerTag);
            });
        }

        started = true;
        LoggerUtil.info("🚀 Update Status worker started with " + concurrentConsumers + " consumers");
    }

    private void processMessage(Delivery delivery) throws Exception {
        long count = processedCount.incrementAndGet();
        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
        LoggerUtil.info("📩 Processing message #" + count + ": " + messageBody);

        JSONObject message;
        try {
            message = new JSONObject(messageBody);
        } catch (JSONException e) {
            LoggerUtil.error("❌ Failed to parse message", e);
            safeReject(delivery.getEnvelope().getDeliveryTag(), false);
            return;
        }

        if (!message.has("messenger_bot_broadcast_serial") ||
            !message.has("messenger_bot_broadcast_serial_send") ||
            !message.has("response")) {
            LoggerUtil.error("❌ Missing required fields in message");
            safeReject(delivery.getEnvelope().getDeliveryTag(), false);
            return;
        }

        int campaignId = message.getInt("messenger_bot_broadcast_serial");
        int messageId = message.getInt("messenger_bot_broadcast_serial_send");
        JSONObject response = message.getJSONObject("response");

        try {
            if (response.has("error")) {
                updateMessageWithError(campaignId, messageId, response.getJSONObject("error"));
                errorCount.incrementAndGet();
            } else {
                updateMessageWithSuccess(campaignId, messageId, response.optString("message_id", ""));
                successCount.incrementAndGet();
            }

            safeAck(delivery.getEnvelope().getDeliveryTag());
            logPeriodicStats();

        } catch (SQLException e) {
            LoggerUtil.error("❌ Database error", e);
            safeReject(delivery.getEnvelope().getDeliveryTag(), true);
        }
    }

    private void updateMessageWithSuccess(int campaignId, int messageId, String externalMessageId) throws SQLException {
        String sql = "UPDATE messenger_bot_broadcast_serial_send " +
                     "SET processed = 1, delivered = 1, message_sent_id = ?, processed_by = 'java-ms-v2'" +
                     "WHERE campaign_id = ? AND id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, externalMessageId);
            stmt.setInt(2, campaignId);
            stmt.setInt(3, messageId);
            stmt.executeUpdate();
        }
    }

    private void updateMessageWithError(int campaignId, int messageId, JSONObject error) throws SQLException {
        String errorMessage = error.optString("message", "Unknown error");
        int errorCode = error.optInt("code", 0);

        String updateSql = "UPDATE messenger_bot_broadcast_serial_send " +
                           "SET processed = 1, delivered = 0, error_message = ?, processed_by = 'java-ms-v2'" +
                           "WHERE campaign_id = ? AND id = ?";
        try (PreparedStatement stmt = dbConnection.prepareStatement(updateSql)) {
            stmt.setString(1, "Code: " + errorCode + ", Message: " + errorMessage);
            stmt.setInt(2, campaignId);
            stmt.setInt(3, messageId);
            stmt.executeUpdate();
        }

        if (errorCode == 551) {
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
                stmt.executeUpdate();
            }
        }
    }

    private void safeAck(long deliveryTag) {
        try {
            if (!channel.isOpen()) reconnectChannel();
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LoggerUtil.error("❌ Failed to ack, even after reconnecting", e);
        }
    }

    private void safeReject(long deliveryTag, boolean requeue) {
        try {
            if (!channel.isOpen()) reconnectChannel();
            channel.basicReject(deliveryTag, requeue);
        } catch (Exception e) {
            LoggerUtil.error("❌ Failed to reject, even after reconnecting", e);
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

    public static void main(String[] args) {
        try {
            WorkerUpdateStatus worker = new WorkerUpdateStatus();
            worker.initialize();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (worker.dbConnection != null) worker.dbConnection.close();
                } catch (Exception e) {
                    LoggerUtil.error("❌ Error shutting down", e);
                }
            }));
            worker.start();
            Thread.currentThread().join();
        } catch (Exception e) {
            LoggerUtil.error("❌ Error starting worker", e);
            System.exit(1);
        }
    }
}
