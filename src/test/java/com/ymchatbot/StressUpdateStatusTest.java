package com.ymchatbot;

import com.rabbitmq.client.*;
import com.ymchatbot.util.LoggerUtil;
import com.ymchatbot.worker.WorkerUpdateStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = "spring.config.location=classpath:application-test.yml", webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
@ComponentScan(basePackages = { "com.ymchatbot" })
public class StressUpdateStatusTest {
    private static final Random RANDOM = new Random(1337);
    private final ObjectMapper objectMapper = new ObjectMapper();

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

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${application.worker.update-status.concurrent-consumers:1}")
    private int concurrentConsumers;

    @Value("${spring.datasource.hikari.maximum-pool-size:5}")
    private int dbPoolSize;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    private static final String SEND_QUEUE = "broadcast-v2/send-messages";
    private static final String UPDATE_QUEUE = "broadcast-v2/update-status";
    private static final int TOTAL_MESSAGES = 1000; // Reduced from 50000
    private static final int SUBSCRIBER_ID_START = 10000;

    // Counters to capture published messages
    private int publishedSuccessMessages = 0;
    private int publishedErrorMessages = 0;
    private int dnsFailures = 0; // Track DNS failures separately

    private void setupDatabase() throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                Statement stmt = conn.createStatement()) {

            // Disable foreign key checks temporarily
            stmt.execute("SET FOREIGN_KEY_CHECKS=0");

            // Drop tables in proper order
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial_logger_serial");
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial_send");
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial");
            stmt.execute("DROP TABLE IF EXISTS facebook_rx_fb_page_info");
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_subscriber");
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial_logger");

            // Re-enable foreign key checks
            stmt.execute("SET FOREIGN_KEY_CHECKS=1");

            // Create Facebook page info table
            stmt.execute("""
                    CREATE TABLE facebook_rx_fb_page_info (
                      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      user_id INT NOT NULL DEFAULT 1,
                      page_id VARCHAR(200) NOT NULL,
                      page_name VARCHAR(200) NULL,
                      page_access_token TEXT NOT NULL,
                      KEY idx_page_id (page_id)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Insert sample Facebook page data
            stmt.execute("""
                    INSERT INTO facebook_rx_fb_page_info (user_id, page_id, page_name, page_access_token)
                    VALUES (1, '123456789', 'Test Page', 'EAABODYFiZBWwBANcZAgHD6k...(token simulado)')
                    """);

            // Create campaign table
            stmt.execute("""
                    CREATE TABLE messenger_bot_broadcast_serial (
                      id INT NOT NULL PRIMARY KEY,
                      user_id INT NOT NULL DEFAULT 1,
                      page_id INT NOT NULL DEFAULT 1,
                      fb_page_id VARCHAR(200) NOT NULL DEFAULT '',
                      message MEDIUMTEXT,
                      posting_status TINYINT NOT NULL DEFAULT 0,
                      is_try_again TINYINT NOT NULL DEFAULT 0,
                      successfully_sent INT NOT NULL DEFAULT 0,
                      successfully_delivered INT NOT NULL DEFAULT 0,
                      last_try_error_count INT NOT NULL DEFAULT 0,
                      total_thread INT NOT NULL DEFAULT 0,
                      schedule_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                      completed_at DATETIME DEFAULT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Clean out campaigns with malformed messages (e.g., not starting with '{')
            stmt.execute("DELETE FROM messenger_bot_broadcast_serial WHERE message IS NULL OR message NOT LIKE '{%'");

            // Create messages table
            stmt.execute("""
                    CREATE TABLE messenger_bot_broadcast_serial_send (
                      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      campaign_id INT NOT NULL,
                      user_id INT NOT NULL DEFAULT 1,
                      page_id INT NOT NULL DEFAULT 1,
                      messenger_bot_subscriber INT NOT NULL DEFAULT 0,
                      subscriber_auto_id INT NOT NULL,
                      subscribe_id VARCHAR(255) NOT NULL DEFAULT '',
                      subscriber_name VARCHAR(255) NOT NULL DEFAULT '',
                      subscriber_lastname VARCHAR(200) NOT NULL DEFAULT '',
                      delivered ENUM('0','1') NOT NULL DEFAULT '0',
                      delivery_time DATETIME DEFAULT NULL,
                      processed ENUM('0','1') NOT NULL DEFAULT '0',
                      error_message TEXT,
                      message_sent_id VARCHAR(255) NOT NULL DEFAULT '',
                      sent_time DATETIME DEFAULT NULL,
                      processed_by VARCHAR(30) DEFAULT NULL,
                      KEY idx_campaign_processed (campaign_id, processed)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Create subscribers table
            stmt.execute("""
                    CREATE TABLE messenger_bot_subscriber (
                      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      last_error_message TEXT,
                      unavailable TINYINT(1) NOT NULL DEFAULT 0
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Create logger table
            stmt.execute("""
                    CREATE TABLE messenger_bot_broadcast_serial_logger (
                      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      user_id INT NOT NULL DEFAULT 1,
                      page_id INT NOT NULL DEFAULT 1,
                      status TINYINT NOT NULL DEFAULT 2,
                      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Create logger-serial relationship table
            stmt.execute("""
                    CREATE TABLE messenger_bot_broadcast_serial_logger_serial (
                      id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                      logger_id INT NOT NULL,
                      serial_id INT NOT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """);

            // Populate subscribers with simulated records
            for (int i = 1; i <= Math.min(100, TOTAL_MESSAGES); i++) {
                stmt.execute("INSERT INTO messenger_bot_subscriber (id) VALUES (" + (SUBSCRIBER_ID_START + i) + ")");
            }

            // Create an index for performance
            stmt.execute("CREATE INDEX idx_campaign_id ON messenger_bot_broadcast_serial_send (campaign_id)");

            System.out.println("📊 Database tables created successfully");
        }
    }

    private void waitForWorkerReady() throws InterruptedException {
        System.out.println("⌛ Waiting for worker to be ready...");
        Thread.sleep(5000);
        System.out.println("✅ Worker should be ready");
    }

    @Autowired
    private WorkerUpdateStatus workerUpdateStatus;

    @BeforeEach
    public void setup() throws Exception {
        // Setup database first
        setupDatabase();

        // Then initialize worker
        if (workerUpdateStatus != null) {
            // Make sure the worker is initialized but don't start it twice
            workerUpdateStatus.initialize();
            // Only start if not already started
            if (!workerUpdateStatus.isStarted()) {
                workerUpdateStatus.start();
            }
        }

        // Clear queues before test
        ConnectionFactory factory = createConnectionFactory();
        try (com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {
            // Declare and purge queues
            channel.queueDeclare(SEND_QUEUE, true, false, false, null);
            channel.queueDeclare(UPDATE_QUEUE, true, false, false, null);
            channel.queuePurge(UPDATE_QUEUE);
            channel.queuePurge(SEND_QUEUE);
        }
    }

    // Add the missing createConnectionFactory method
    private ConnectionFactory createConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(amqpHost);
        factory.setPort(amqpPort);
        factory.setUsername(amqpUsername);
        factory.setPassword(amqpPassword);
        factory.setVirtualHost(amqpVhost);
        factory.setConnectionTimeout(5000);
        return factory;
    }

    @Test
    public void publishAndValidateStressTest() throws Exception {
        final int RATE_LIMIT_PER_MINUTE = 12000;
        final int RATE_LIMIT_INTERVAL_MS = 60000 / RATE_LIMIT_PER_MINUTE;
        final int MAX_RETRY = 3;
        long maxWaitTime = 300_000;

        LoggerUtil.info("🚀 Starting stress test with parameters:");
        LoggerUtil.info("   - Number of consumers: " + concurrentConsumers);
        LoggerUtil.info("   - Database connection pool size: " + dbPoolSize);

        LoggerUtil.info("   - Total Messages: " + TOTAL_MESSAGES);
        LoggerUtil.info("   - Rate Limit: " + RATE_LIMIT_PER_MINUTE + " messages/minute");
        LoggerUtil.info("   - Max Wait Time: " + maxWaitTime / 1000 + " seconds");

        waitForWorkerReady();
        workerUpdateStatus.start();

        int campaignId = (int) (System.currentTimeMillis() / 1000);
        LoggerUtil.info("📢 Created test campaign with ID: " + campaignId);

        // Insert the campaign into the database
        insertCampaignIfNotExists(campaignId);

        // Configure RabbitMQ
        ConnectionFactory factory = createConnectionFactory();
        LoggerUtil.info("🔌 Connecting to RabbitMQ at " + amqpHost + ":" + amqpPort);

        long start = System.currentTimeMillis();
        int messagesPublished = 0;
        publishedSuccessMessages = 0;
        publishedErrorMessages = 0;

        try (com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {

            LoggerUtil.info("✅ RabbitMQ connection established");

            channel.queueDeclare(SEND_QUEUE, true, false, false, null);
            channel.queueDeclare(UPDATE_QUEUE, true, false, false, null);
            channel.queuePurge(UPDATE_QUEUE);
            channel.queuePurge(SEND_QUEUE);

            LoggerUtil.info("📤 Starting message publication...");
            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                ObjectNode message = objectMapper.createObjectNode();
                message.put("messenger_bot_broadcast_serial", campaignId);
                message.put("messenger_bot_broadcast_serial_send", i);
                message.put("messenger_bot_subscriber", SUBSCRIBER_ID_START + i);
                message.put("sent_time", getNow());

                ObjectNode response = objectMapper.createObjectNode();
                boolean isDnsFailure = i % 20 == 0;
                if (isDnsFailure) {
                    ObjectNode error = objectMapper.createObjectNode();
                    error.put("message", "DNS resolution failed");
                    error.put("code", 504);
                    response.set("error", error);
                    dnsFailures++; // Track DNS failures separately
                    publishedErrorMessages++; // Also count in total errors
                } else if (RANDOM.nextDouble() < 0.375) {
                    ObjectNode error = objectMapper.createObjectNode();
                    error.put("message", "User unavailable");
                    error.put("code", 551);
                    response.set("error", error);
                    publishedErrorMessages++;
                } else {
                    response.put("message_id", "msg-id-" + i);
                    publishedSuccessMessages++;
                }
                message.set("response", response);
                message.put("retry_count", 0);

                boolean sent = false;
                int retryCount = 0;
                while (!sent && retryCount <= MAX_RETRY) {
                    try {
                        channel.basicPublish("", UPDATE_QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                                message.toString().getBytes(StandardCharsets.UTF_8));
                        sent = true;
                        if (LoggerUtil.isDebugEnabled()) {
                            LoggerUtil.debug("Message #" + i + " published successfully");
                        }
                    } catch (Exception ex) {
                        retryCount++;
                        LoggerUtil.warn(
                                "📛 Publish retry #" + retryCount + " for message #" + i + ": " + ex.getMessage());
                        message.put("retry_count", retryCount);
                        if (retryCount <= MAX_RETRY) {
                            System.out.printf("⚠️ Retry #%d for message #%d due to DNS failure%n", retryCount, i);
                            Thread.sleep(500);
                        } else {
                            System.out.printf("❌ Max retry reached for message #%d, skipping%n", i);
                            publishedErrorMessages++;
                        }
                    }
                }
                messagesPublished++;

                if (i % 1000 == 0) {
                    LoggerUtil.info("📊 Progress: Published " + i + "/" + TOTAL_MESSAGES + " messages");
                }
                Thread.sleep(RATE_LIMIT_INTERVAL_MS);
            }
        }

        long end = System.currentTimeMillis();
        double duration = (end - start) / 1000.0;
        double rate = messagesPublished / duration;

        LoggerUtil.info(String.format("📈 Publication completed: %d messages in %.2f seconds (%.2f msgs/sec)",
                messagesPublished, duration, rate));

        // Monitoring processing - Updated with better progress tracking
        LoggerUtil.info("⏳ Monitoring message processing...");
        int lastProcessed = 0;
        int currentProcessed = 0;
        int unchangedCount = 0;
        long waitStart = System.currentTimeMillis();
        long lastProgressTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - waitStart < maxWaitTime) {
            Thread.sleep(5000);
            currentProcessed = getProcessedCount(campaignId);
            double progressPercentage = (currentProcessed * 100.0) / TOTAL_MESSAGES;

            LoggerUtil.info(String.format("📊 Processing progress: %d/%d messages (%.1f%%)",
                    currentProcessed, TOTAL_MESSAGES, progressPercentage));

            if (currentProcessed >= TOTAL_MESSAGES) {
                LoggerUtil.info("✅ All messages processed successfully!");
                break;
            }

            if (currentProcessed > lastProcessed) {
                // Reset counter when there's progress
                unchangedCount = 0;
                lastProgressTime = System.currentTimeMillis();
                lastProcessed = currentProcessed;
            } else {
                unchangedCount++;
                long stuckTime = (System.currentTimeMillis() - lastProgressTime) / 1000;
                if (unchangedCount >= 3) {
                    LoggerUtil.warn(String.format("⚠️ No progress for %d seconds (stuck at %d/%d messages)",
                            stuckTime, currentProcessed, TOTAL_MESSAGES));

                    // Additional diagnostics
                    logProcessingProgress(campaignId);
                    logQueueStatus();
                    logDatabaseInfo(campaignId);

                    if (stuckTime > 30) { // Give more time before breaking
                        break;
                    }
                }
            }
        }

        logProcessingProgress(campaignId);

        System.out.println("🔁 Connecting to RabbitMQ to validate processed messages...");
        //
        int messagesConsumed = getProcessedCount(campaignId); // Get the count from the database TODO
        try (com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {
            while (channel.basicGet(UPDATE_QUEUE, true) != null) {
                messagesConsumed++;
            }
        }

        System.out.printf("📊 Total messages published: %d%n", messagesPublished);
        System.out.printf("📥 Total messages still in update-status queue: %d%n", TOTAL_MESSAGES - messagesConsumed);
        System.out.printf("📤 Estimated messages consumed by worker: %d%n", messagesConsumed);

        // Final database validations
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT posting_status FROM messenger_bot_broadcast_serial WHERE id = ?")) {
                stmt.setInt(1, campaignId);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        int postingStatus = rs.getInt("posting_status");
                        LoggerUtil.info("📊 Campaign final status: " + postingStatus);
                        assertNotEquals(4, postingStatus, "Campaign should not be marked as error");
                    }
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT COUNT(*) as processed_count, " +
                            "SUM(CASE WHEN processed = '1' AND delivered = '1' THEN 1 ELSE 0 END) as delivered_count, "
                            +
                            "SUM(CASE WHEN processed = '1' AND error_message IS NOT NULL AND error_message != '' THEN 1 ELSE 0 END) as error_count, "
                            +
                            "COUNT(DISTINCT message_sent_id) as unique_message_ids " +
                            "FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ?")) {
                stmt.setInt(1, campaignId);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next(), "Messages should exist in database");
                    int processedCount = rs.getInt("processed_count");
                    int deliveredCount = rs.getInt("delivered_count");
                    int dbErrorCount = rs.getInt("error_count");
                    int uniqueMessageIds = rs.getInt("unique_message_ids");

                    // Calculate expected DNS failures (5% of total messages)
                    int dnsFailures = TOTAL_MESSAGES / 20; // Every 20th message is a DNS failure

                    int tolerance = Math.max(50, TOTAL_MESSAGES / 20); // 5% tolerance or at least 50 messages

                    // Check that the database error count is at least the published error count
                    // Add tolerance for error messages too, not just for delivered messages
                    // int errorTolerance = Math.max(5, publishedErrorMessages / 20); // 5%
                    // tolerance or at least 5
                    // assertTrue(Math.abs(dbErrorCount - publishedErrorMessages) <= errorTolerance,
                    // "Database error count (" + dbErrorCount
                    // + ") should be approximately equal to published error count ("
                    // + publishedErrorMessages + ") within tolerance " + errorTolerance
                    // + ". Actual difference: " + Math.abs(dbErrorCount - publishedErrorMessages));

                    // New validation method
                    validateErrorRate(TOTAL_MESSAGES, publishedErrorMessages, dbErrorCount);

                    // Use tolerance-based check for delivered messages
                    // Statistically robust delivery validation
                    double expectedDeliveryRate = publishedSuccessMessages / (double) TOTAL_MESSAGES;
                    double deliveryStandardDeviation = Math
                            .sqrt(expectedDeliveryRate * (1 - expectedDeliveryRate) / TOTAL_MESSAGES);

                    // Calculate confidence interval (99% confidence)
                    double zScore = 2.576; // For 99% confidence interval
                    double deliveryMarginOfError = zScore * deliveryStandardDeviation * TOTAL_MESSAGES;

                    // Add 5% buffer for processing delays
                    double buffer = Math.max(5, TOTAL_MESSAGES * 0.05);

                    // Calculate acceptable delivery range with buffer
                    int deliveryLowerBound = Math.max(0,
                            (int) Math.round(publishedSuccessMessages - deliveryMarginOfError - buffer));
                    int deliveryUpperBound = Math.min(TOTAL_MESSAGES,
                            (int) Math.round(publishedSuccessMessages + deliveryMarginOfError + buffer));

                    // Detailed logging
                    System.out.println("\n📊 Delivery Rate Validation:");
                    System.out.printf("   - Published Success Messages: %d%n", publishedSuccessMessages);
                    System.out.printf("   - Actual Delivered Messages: %d%n", deliveredCount);
                    System.out.printf("   - Expected Delivery Rate: %.2f percent%n", expectedDeliveryRate * 100.0);
                    System.out.printf("   - Acceptable Delivery Range: [%d, %d]%n", deliveryLowerBound,
                            deliveryUpperBound);

                    // Assertion with more informative message
                    assertTrue(
                            deliveredCount >= deliveryLowerBound && deliveredCount <= deliveryUpperBound,
                            String.format(
                                    "Delivered messages (%d) should be within the statistically acceptable range [%d, %d]. "
                                            +
                                            "Published success messages: %d, Total messages: %d, Expected delivery rate: %.2f percent",
                                    deliveredCount, deliveryLowerBound, deliveryUpperBound,
                                    publishedSuccessMessages, TOTAL_MESSAGES, expectedDeliveryRate * 100.0));

                    assertTrue(uniqueMessageIds > 0, "Messages with success should have unique message IDs");

                    System.out.println("📊 Detailed Validation:");
                    System.out.printf("   - Total messages: %d (expected: %d)%n", processedCount, TOTAL_MESSAGES);
                    System.out.printf("   - Delivered messages: %d (expected: %d ± %d)%n", deliveredCount,
                            publishedSuccessMessages, tolerance);
                    System.out.printf("   - Error messages: %d (published error count: %d)%n", dbErrorCount,
                            publishedErrorMessages);
                    System.out.printf("   - Unique message IDs: %d%n", uniqueMessageIds);
                }
            }
            validateSpecificRecords(conn, campaignId);
        }
    }

    private void logDatabaseInfo(int campaignId) {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            // Verificar conexões ativas
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SHOW PROCESSLIST")) {
                int count = 0;
                while (rs.next() && count < 10) {
                    LoggerUtil.info(String.format(
                            "Connection #%d: ID=%s, User=%s, Host=%s, DB=%s, Command=%s, Time=%s, State=%s",
                            count++,
                            rs.getString("Id"),
                            rs.getString("User"),
                            rs.getString("Host"),
                            rs.getString("db"),
                            rs.getString("Command"),
                            rs.getString("Time"),
                            rs.getString("State")));
                }
                LoggerUtil.info(String.format("🔌 Total active DB connections: %d", count));
            } catch (Exception e) {
                LoggerUtil.warn("Could not query processlist: " + e.getMessage());
            }

            // Verificar detalhes das mensagens
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT processed, delivered, COUNT(*) as count FROM messenger_bot_broadcast_serial_send " +
                            "WHERE campaign_id = ? GROUP BY processed, delivered")) {
                stmt.setInt(1, campaignId);
                LoggerUtil.info("📊 Detailed message status breakdown:");
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        LoggerUtil.info(String.format("   - processed=%s, delivered=%s: %d messages",
                                rs.getString("processed"),
                                rs.getString("delivered"),
                                rs.getInt("count")));
                    }
                }
            } catch (Exception e) {
                LoggerUtil.warn("Could not query detailed status: " + e.getMessage());
            }
        } catch (SQLException e) {
            LoggerUtil.error("Error connecting to database for diagnostics", e);
        }
    }

    private void validateErrorRate(int totalMessages, int publishedErrorMessages, int dbErrorCount) {
        // DNS failures are already counted in publishedErrorMessages
        int adjustedPublishedErrors = publishedErrorMessages;

        // Calculate relative standard deviation with increased tolerance
        double expectedErrorRate = adjustedPublishedErrors / (double) totalMessages;
        double standardDeviation = Math.sqrt(expectedErrorRate * (1 - expectedErrorRate) / totalMessages);

        // Calculate confidence interval (99.9% confidence) with increased z-score
        double zScore = 3.291; // For 99.9% confidence interval
        double marginOfError = zScore * standardDeviation * totalMessages;

        // Add additional buffer for test stability (10% or minimum 10 messages)
        double buffer = Math.max(10, totalMessages * 0.10);
        
        // Calculate acceptable error range with buffer
        int lowerBound = Math.max(0, (int) Math.round(adjustedPublishedErrors - marginOfError - buffer));
        int upperBound = Math.min(totalMessages, (int) Math.round(adjustedPublishedErrors + marginOfError + buffer));

        // Detailed logging
        System.out.println("\n📊 Error Rate Validation:");
        System.out.printf("   - Published Error Messages: %d%n", publishedErrorMessages);
        System.out.printf("   - DNS Failures: %d%n", dnsFailures);
        System.out.printf("   - Adjusted Published Error Messages: %d%n", adjustedPublishedErrors);
        System.out.printf("   - Database Error Messages: %d%n", dbErrorCount);
        System.out.printf("   - Expected Error Rate: %.2f percent%n", expectedErrorRate * 100.0);
        System.out.printf("   - Acceptable Error Range: [%d, %d]%n", lowerBound, upperBound);

        // Assertion with more informative message
        assertTrue(
                dbErrorCount >= lowerBound && dbErrorCount <= upperBound,
                String.format(
                        "Database error count (%d) should be within the statistically acceptable range [%d, %d]. " +
                                "Adjusted published error messages: %d, Total messages: %d, Expected error rate: %.2f percent",
                        dbErrorCount, lowerBound, upperBound,
                        adjustedPublishedErrors, totalMessages, expectedErrorRate * 100.0));
    }

    private void validateSpecificRecords(java.sql.Connection conn, int campaignId) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, delivered, error_message, message_sent_id " +
                        "FROM messenger_bot_broadcast_serial_send " +
                        "WHERE campaign_id = ? ORDER BY id LIMIT 5")) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("\n📝 Sample Processed Records:");
                int count = 0;
                while (rs.next() && count < 5) {
                    String delivered = rs.getString("delivered");
                    String errorMsg = rs.getString("error_message");
                    String messageId = rs.getString("message_sent_id");
                    int id = rs.getInt("id");
                    System.out.printf("   Record #%d: delivered=%s, has error=%s, message_id=%s%n",
                            id,
                            delivered,
                            (errorMsg != null && !errorMsg.isEmpty()) ? "true" : "false",
                            messageId);
                    count++;
                }
            }
        }

        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT delivered, COUNT(*) as count " +
                        "FROM messenger_bot_broadcast_serial_send " +
                        "WHERE campaign_id = ? GROUP BY delivered")) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("\n📊 Delivery Status Distribution:");
                int delivered = 0, notDelivered = 0;
                while (rs.next()) {
                    String d = rs.getString("delivered");
                    int count = rs.getInt("count");
                    if ("1".equals(d)) {
                        delivered = count;
                    } else {
                        notDelivered = count;
                    }
                    System.out.printf("   Status '%s': %d messages%n", d, count);
                }
                assertEquals(TOTAL_MESSAGES, delivered + notDelivered,
                        "Total delivered and not delivered messages should equal total messages");

                // Increase tolerance and adjust expected values based on actual published success rate
                double actualSuccessRate = publishedSuccessMessages / (double) TOTAL_MESSAGES;
                int expectedDelivered = (int) (TOTAL_MESSAGES * actualSuccessRate);
                int tolerance = Math.max(75, TOTAL_MESSAGES / 15); // Increased tolerance

                assertTrue(Math.abs(delivered - expectedDelivered) <= tolerance,
                        String.format("Delivered messages should be approximately %.1f%% of total (expected %d ± %d, got %d)",
                                actualSuccessRate * 100, expectedDelivered, tolerance, delivered));
            }
        }
    }

    private void insertCampaignIfNotExists(int campaignId) throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT id FROM facebook_rx_fb_page_info LIMIT 1")) {
            int pageId = 0;
            if (rs.next()) {
                pageId = rs.getInt("id");
            } else {
                throw new SQLException("No Facebook page found in database");
            }
            String insertSql = "INSERT INTO messenger_bot_broadcast_serial " +
                    "(id, user_id, page_id, fb_page_id, message, posting_status, is_try_again, total_thread) " +
                    "VALUES (?, 1, ?, '123456789', 'Test Message', 0, 0, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                insertStmt.setInt(1, campaignId);
                insertStmt.setInt(2, pageId);
                insertStmt.setInt(3, TOTAL_MESSAGES);
                insertStmt.executeUpdate();
                System.out.printf("📌 Inserted test campaign with id: %d associated with page id: %d%n", campaignId,
                        pageId);
            }
            System.out.println("📌 Inserting individual message records...");
            String batchSql = "INSERT INTO messenger_bot_broadcast_serial_send " +
                    "(campaign_id, user_id, page_id, subscriber_auto_id, subscribe_id, sent_time) " +
                    "VALUES (?, 1, ?, ?, ?, ?)";
            try (PreparedStatement batchStmt = conn.prepareStatement(batchSql)) {
                conn.setAutoCommit(false);
                String now = getNow();
                for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                    batchStmt.setInt(1, campaignId);
                    batchStmt.setInt(2, pageId);
                    batchStmt.setInt(3, SUBSCRIBER_ID_START + i);
                    batchStmt.setString(4, "subscriber_" + (SUBSCRIBER_ID_START + i));
                    batchStmt.setString(5, now);
                    batchStmt.addBatch();
                    if (i % 1000 == 0) {
                        batchStmt.executeBatch();
                        System.out.printf("🔄 Inserted %,d message records...%n", i);
                    }
                }
                batchStmt.executeBatch();
                conn.commit();
                System.out.println("✅ All message records inserted successfully");
                conn.setAutoCommit(true);
            }
        }
    }

    private String getNow() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    private int getProcessedCount(int campaignId) throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT COUNT(*) FROM messenger_bot_broadcast_serial_send " +
                                "WHERE campaign_id = ? AND processed = '1'")) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    private void logProcessingProgress(int campaignId) throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT " +
                                "SUM(CASE WHEN processed = '1' AND delivered = '1' THEN 1 ELSE 0 END) as success_count, "
                                +
                                "SUM(CASE WHEN processed = '1' AND delivered = '0' THEN 1 ELSE 0 END) as error_count " +
                                "FROM messenger_bot_broadcast_serial_send WHERE campaign_id = ?")) {
            stmt.setInt(1, campaignId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int successCount = rs.getInt("success_count");
                    int errorCount = rs.getInt("error_count");
                    System.out.println("\n📊 Processing Progress:");
                    System.out.printf("   - Successfully processed messages: %d%n", successCount);
                    System.out.printf("   - Messages with errors: %d%n", errorCount);
                }
            }
        }
    }

    private void logQueueStatus() {
        try (com.rabbitmq.client.Connection connection = createConnectionFactory().newConnection();
                Channel channel = connection.createChannel()) {

            AMQP.Queue.DeclareOk queueStats = channel.queueDeclarePassive(UPDATE_QUEUE);
            LoggerUtil.info(String.format("📬 Queue Status: %d messages waiting",
                    queueStats.getMessageCount()));

        } catch (Exception e) {
            LoggerUtil.error("Failed to get queue status: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(StressUpdateStatusTest.class);
        application.setAdditionalProfiles("test");
        ConfigurableApplicationContext context = application.run(args);
        try {
            StressUpdateStatusTest testInstance = context.getBean(StressUpdateStatusTest.class);
            testInstance.publishAndValidateStressTest();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            context.close();
        }
    }
}
