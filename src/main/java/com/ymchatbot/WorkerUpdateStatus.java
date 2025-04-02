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
    private static final int MAX_DB_RECONNECT_ATTEMPTS = 3;

    // Add flag to track initialization and started state
    private volatile boolean initialized = false;
    private volatile boolean started = false;

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

    // Reduzir o número de consumidores para o teste
    @Value("${application.worker.update-status.concurrent-consumers:1}")
    private int concurrentConsumers;

    private java.sql.Connection dbConnection;
    private Channel channel;
    private com.rabbitmq.client.Connection connection;

    private static AtomicLong processedCount = new AtomicLong(0);
    private static AtomicLong successCount = new AtomicLong(0);
    private static AtomicLong errorCount = new AtomicLong(0);
    private static long lastStatsLogTime = 0;
    private static final long STATS_LOG_INTERVAL_MS = 10000; // Reduzido para 10 segundos

    @PostConstruct
    public void initFromSpring() throws Exception {
        if (!initialized)
            initialize();
        if (!started)
            start();
    }

    public synchronized void initialize() throws Exception {
        if (initialized) {
            LoggerUtil.info("WorkerUpdateStatus already initialized, skipping initialization");
            return;
        }

        LoggerUtil.info("⏳ Initializing WorkerUpdateStatus...");

        try {
            // Initialize database connection
            initDbConnection();

            // Initialize RabbitMQ connection
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(amqpHost);
            factory.setPort(amqpPort);
            factory.setUsername(amqpUsername);
            factory.setPassword(amqpPassword);
            factory.setVirtualHost(amqpVhost);

            // Aumentar tempo de conexão
            factory.setConnectionTimeout(10000);
            factory.setRequestedHeartbeat(30); // heartbeat em segundos

            connection = factory.newConnection("worker-update-status");
            channel = connection.createChannel();

            // Declare queue with durable flag
            boolean durable = true;
            channel.queueDeclare(queueName, durable, false, false, null);

            initialized = true;
            LoggerUtil.info("✅ WorkerUpdateStatus initialized successfully");
        } catch (Exception e) {
            LoggerUtil.error("❌ Failed to initialize WorkerUpdateStatus", e);
            throw e;
        }
    }

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

        // Set up consumer with enhanced logging
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try {
                String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                LoggerUtil.info("📩 Received message from update-status queue: " +
                        messageBody.substring(0, Math.min(100, messageBody.length())) + "...");

                // Processar a mensagem
                boolean processed = processMessage(delivery);

                try {
                    // Verificar canal antes de confirmar ou rejeitar
                    if (channel == null || !channel.isOpen()) {
                        LoggerUtil.warn("⚠️ Channel closed before acknowledging message, reconnecting...");
                        reconnectChannel();
                    }

                    if (processed) {
                        LoggerUtil.info("✅ Message processed successfully, acknowledging");
                        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    } else {
                        LoggerUtil.info("⚠️ Message processing incomplete, rejecting with requeue");
                        channel.basicReject(delivery.getEnvelope().getDeliveryTag(), true);
                    }
                } catch (IOException ioe) {
                    LoggerUtil.error("❌ Failed to acknowledge/reject message after processing", ioe);
                    // Se falhar ao confirmar, tentar reconectar para a próxima mensagem
                    reconnectChannel();
                }
            } catch (Exception e) {
                LoggerUtil.error("❌ Error processing message from update-status queue: " + e.getMessage(), e);
                try {
                    // Verificar e reconectar o canal se necessário
                    if (e instanceof IOException || channel == null || !channel.isOpen()) {
                        LoggerUtil.warn("⚠️ Possible connection issue, attempting to reconnect channel...");
                        reconnectChannel();
                    }

                    boolean requeue = shouldRequeueError(e);
                    LoggerUtil.info("Message will be " + (requeue ? "requeued" : "discarded"));
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), requeue);
                } catch (Exception ex) {
                    LoggerUtil.error("❌ Failed to reject message after error: " + ex.getMessage(), ex);
                    // Não podemos fazer muito mais aqui além de logar e seguir em frente
                }
            }
        };

        // Reduzir prefetch para processar menos mensagens simultaneamente
        channel.basicQos(5);

        // Start consuming with multiple consumers
        for (int i = 0; i < concurrentConsumers; i++) {
            String consumerTag = channel.basicConsume(queueName, false, deliverCallback, tag -> {
                LoggerUtil.info("Consumer cancelled: " + tag);
            });
            LoggerUtil.info("👤 Started consumer #" + (i + 1) + " with tag: " + consumerTag);
        }

        started = true;
        LoggerUtil.info("🚀 Update Status worker started with " + concurrentConsumers + " consumers");
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isInitialized() {
        return initialized;
    }

    private void initDbConnection() throws SQLException {
        try {
            LoggerUtil.info("Connecting to database: " + dbUrl);
            dbConnection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
            // Testar a conexão
            try (Statement stmt = dbConnection.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT 1");
                if (rs.next()) {
                    LoggerUtil.info("✅ Database connection test successful");
                }
            }
            LoggerUtil.info("✅ Database connection established to " + dbUrl);
        } catch (SQLException e) {
            LoggerUtil.error("❌ Failed to connect to database: " + e.getMessage(), e);
            throw e;
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

    // Método principal de processamento - agora retorna boolean para indicar
    // sucesso/falha
    private boolean processMessage(Delivery delivery) throws Exception {
        long count = processedCount.incrementAndGet();
        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);

        // Log completo para debug
        LoggerUtil.debug("📄 Full message content: " + messageBody);
        LoggerUtil.info("📩 Processing message #" + count);

        JSONObject message;
        try {
            message = new JSONObject(messageBody);
            LoggerUtil.debug("✅ Message parsed as JSON successfully");
        } catch (JSONException e) {
            LoggerUtil.error("❌ Failed to parse message as JSON: " + e.getMessage(), e);
            // Não confirmar mensagens mal formatadas
            errorCount.incrementAndGet();
            return false; // Retorna falso para indicar que o processamento falhou
        }

        // Validação dos campos obrigatórios
        if (!message.has("messenger_bot_broadcast_serial") ||
                !message.has("messenger_bot_broadcast_serial_send") ||
                !message.has("response")) {
            LoggerUtil.error("❌ Missing required fields in message: " +
                    "has campaign_id=" + message.has("messenger_bot_broadcast_serial") +
                    ", has message_id=" + message.has("messenger_bot_broadcast_serial_send") +
                    ", has response=" + message.has("response"));
            errorCount.incrementAndGet();
            return false; // Retorna falso para indicar que o processamento falhou
        }

        int campaignId = message.getInt("messenger_bot_broadcast_serial");
        int messageId = message.getInt("messenger_bot_broadcast_serial_send");
        JSONObject response = message.getJSONObject("response");

        LoggerUtil.info("🔄 Processing message for campaign=" + campaignId + ", message_id=" + messageId);

        boolean success = false;
        try {
            if (response.has("error")) {
                JSONObject error = response.getJSONObject("error");
                LoggerUtil.info("🔄 Message has error: " + error.toString());
                success = updateMessageWithError(campaignId, messageId, error);
                errorCount.incrementAndGet();
            } else {
                String externalMsgId = response.optString("message_id", "");
                LoggerUtil.info("🔄 Message is success with external_id: " + externalMsgId);
                success = updateMessageWithSuccess(campaignId, messageId, externalMsgId);
                successCount.incrementAndGet();
            }

            logPeriodicStats();
            return success;

        } catch (SQLException e) {
            LoggerUtil.error("❌ Database error processing message " + messageId + " for campaign " + campaignId + ": "
                    + e.getMessage(), e);
            return false;
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

    private boolean updateMessageWithError(int campaignId, int messageId, JSONObject error) throws SQLException {
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

            String errorMessage = error.optString("message", "Unknown error");
            int errorCode = error.optInt("code", 0);

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

        // Verificar se a tabela existe antes de tentar atualizar
        try (Statement checkStmt = dbConnection.createStatement()) {
            ResultSet rs = checkStmt.executeQuery("SHOW TABLES LIKE 'messenger_bot_subscriber'");
            if (!rs.next()) {
                LoggerUtil.warn("⚠️ messenger_bot_subscriber table does not exist, skipping subscriber update");
                return;
            }
        }

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

    private void reconnectChannel() {
        try {
            LoggerUtil.info("🔄 Attempting to reconnect RabbitMQ channel...");
            if (connection == null || !connection.isOpen()) {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(amqpHost);
                factory.setPort(amqpPort);
                factory.setUsername(amqpUsername);
                factory.setPassword(amqpPassword);
                factory.setVirtualHost(amqpVhost);
                factory.setConnectionTimeout(10000);

                connection = factory.newConnection("worker-update-status");
                LoggerUtil.info("✅ RabbitMQ connection reestablished");
            }

            if (channel == null || !channel.isOpen()) {
                channel = connection.createChannel();
                channel.queueDeclare(queueName, true, false, false, null);
                LoggerUtil.info("✅ RabbitMQ channel recreated");
            }

            LoggerUtil.info("✅ RabbitMQ channel reconnected successfully");
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
        if (e instanceof JSONException) {
            LoggerUtil.debug("JSONException will not be requeued: " + e.getMessage());
            return false;
        }

        // Por padrão, requeue erros desconhecidos
        LoggerUtil.debug("Unknown error type will be requeued: " + e.getClass().getName());
        return true;
    }
}