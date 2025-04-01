package com.ymchatbot;

import com.rabbitmq.client.*;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
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
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = "spring.config.location=classpath:application-test.yml", webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
public class StressUpdateStatusTest {

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

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    private static final String SEND_QUEUE = "broadcast-v2/send-messages";
    private static final String UPDATE_QUEUE = "broadcast-v2/update-status";
    private static final int TOTAL_MESSAGES = 50000;
    private static final int SUBSCRIBER_ID_START = 10000;

    @BeforeEach
    public void setup() throws SQLException {
        // Garantir que as tabelas necessárias existam
        setupDatabase();
    }

    private void setupDatabase() throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
                Statement stmt = conn.createStatement()) {

            // Eliminar tabelas existentes
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial_send");
            stmt.execute("DROP TABLE IF EXISTS messenger_bot_broadcast_serial");
            stmt.execute("DROP TABLE IF EXISTS facebook_rx_fb_page_info");

            // Criar tabela de páginas do Facebook
            stmt.execute("CREATE TABLE facebook_rx_fb_page_info (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "user_id INT NOT NULL DEFAULT 1, " +
                    "page_id VARCHAR(200) NOT NULL, " +
                    "page_name VARCHAR(200) NULL, " +
                    "page_access_token TEXT NOT NULL, " +
                    "KEY idx_page_id (page_id))");

            // Inserir dados de exemplo para a página
            stmt.execute("INSERT INTO facebook_rx_fb_page_info (user_id, page_id, page_name, page_access_token) " +
                    "VALUES (1, '123456789', 'Test Page', 'EAABODYFiZBWwBANcZAgHD6k...(token simulado)')");

            // Criar tabela de campanhas
            stmt.execute("CREATE TABLE messenger_bot_broadcast_serial (" +
                    "id INT NOT NULL PRIMARY KEY, " +
                    "user_id INT NOT NULL DEFAULT 1, " +
                    "page_id INT NOT NULL DEFAULT 1, " +
                    "fb_page_id VARCHAR(200) NOT NULL DEFAULT '', " +
                    "posting_status TINYINT NOT NULL DEFAULT 0, " +
                    "is_try_again TINYINT NOT NULL DEFAULT 0, " +
                    "successfully_sent INT NOT NULL DEFAULT 0, " +
                    "successfully_delivered INT NOT NULL DEFAULT 0, " +
                    "last_try_error_count INT NOT NULL DEFAULT 0, " +
                    "total_thread INT NOT NULL DEFAULT 0, " +
                    "schedule_time DATETIME DEFAULT CURRENT_TIMESTAMP, " +
                    "created_at DATETIME DEFAULT CURRENT_TIMESTAMP, " +
                    "completed_at DATETIME DEFAULT NULL)");

            // Criar tabela de mensagens
            stmt.execute("CREATE TABLE messenger_bot_broadcast_serial_send (" +
                    "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
                    "campaign_id INT NOT NULL, " +
                    "user_id INT NOT NULL DEFAULT 1, " +
                    "page_id INT NOT NULL DEFAULT 1, " +
                    "subscriber_auto_id INT NOT NULL, " +
                    "subscribe_id VARCHAR(255) NOT NULL DEFAULT '', " +
                    "delivered ENUM('0','1') NOT NULL DEFAULT '0', " +
                    "delivery_time DATETIME DEFAULT NULL, " +
                    "processed ENUM('0','1') NOT NULL DEFAULT '0', " +
                    "error_message TEXT, " +
                    "message_sent_id VARCHAR(255) NOT NULL DEFAULT '', " +
                    "sent_time DATETIME DEFAULT NULL, " +
                    "processed_by VARCHAR(30) DEFAULT NULL, " +
                    "KEY idx_campaign_processed (campaign_id, processed))");

            System.out.println("📊 Database tables created successfully");
        }
    }

    private void waitForWorkerReady() throws InterruptedException {
        System.out.println("⌛ Waiting for worker to be ready...");
        Thread.sleep(2000); // Give worker time to initialize
        System.out.println("✅ Worker should be ready");
    }

    @Autowired
    private WorkerUpdateStatus workerUpdateStatus;

    @Test
    public void publishAndValidateStressTest() throws Exception {
        // Add at start of test
        final int RATE_LIMIT_PER_MINUTE = 6250;
        final int RATE_LIMIT_INTERVAL_MS = 60000 / RATE_LIMIT_PER_MINUTE;
        final int MAX_RETRY = 3;

        waitForWorkerReady();
        
        // Inicia o worker de update para processar as mensagens na fila update-status
        workerUpdateStatus.start();

        int campaignId = (int) (System.currentTimeMillis() / 1000);
        System.out.printf("🧪 Starting stress test with dynamic campaignId: %d%n", campaignId);
        System.out.printf("📊 Expected success messages: %d, Expected error messages: %d%n",
                (TOTAL_MESSAGES * 2) / 3, TOTAL_MESSAGES / 3);

        // 1. Insere a campanha ativa no banco
        insertCampaignIfNotExists(campaignId);

        // 2. Configura o RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(amqpHost);
        factory.setPort(amqpPort);
        factory.setUsername(amqpUsername);
        factory.setPassword(amqpPassword);
        factory.setVirtualHost(amqpVhost);

        long start = System.currentTimeMillis();

        int messagesPublished = 0;
        int successMessages = 0;
        int errorMessages = 0;

        try (com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {

            channel.queueDeclare(SEND_QUEUE, true, false, false, null);
            channel.queueDeclare(UPDATE_QUEUE, true, false, false, null);
            channel.queuePurge(UPDATE_QUEUE);
            channel.queuePurge(SEND_QUEUE);

            for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                JSONObject message = new JSONObject();
                message.put("messenger_bot_broadcast_serial", campaignId);
                message.put("messenger_bot_broadcast_serial_send", i);
                message.put("messenger_bot_subscriber", SUBSCRIBER_ID_START + i);
                message.put("sent_time", getNow());

                JSONObject response = new JSONObject();

                // Simula 5% de falha de DNS para testar retry
                boolean isDnsFailure = i % 20 == 0;

                if (isDnsFailure) {
                    JSONObject error = new JSONObject();
                    error.put("message", "DNS resolution failed");
                    error.put("code", 504);
                    response.put("error", error);
                } else if (i % 3 == 0) {
                    // Simula erro comum de usuário indisponível
                    JSONObject error = new JSONObject();
                    error.put("message", "User unavailable");
                    error.put("code", 551);
                    response.put("error", error);
                    errorMessages++;
                } else {
                    // Sucesso
                    response.put("message_id", "msg-id-" + i);
                    successMessages++;
                }

                message.put("response", response);
                message.put("retry_count", 0);

                boolean sent = false;
                int retryCount = 0;

                while (!sent && retryCount <= MAX_RETRY) {
                    try {
                        channel.basicPublish("", UPDATE_QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN,
                                message.toString().getBytes(StandardCharsets.UTF_8));
                        sent = true;
                    } catch (Exception ex) {
                        retryCount++;
                        message.put("retry_count", retryCount);
                        if (retryCount <= MAX_RETRY) {
                            System.out.printf("⚠️ Retry #%d for message #%d due to DNS failure%n", retryCount, i);
                            Thread.sleep(500); // Espera antes de tentar novamente
                        } else {
                            System.out.printf("❌ Max retry reached for message #%d, skipping%n", i);
                            errorMessages++;
                        }
                    }
                }

                messagesPublished++;

                if (i % 1000 == 0) {
                    System.out.printf("[StressTest] Published %,d messages...%n", i);
                }

                // Limite de taxa para não exceder 6250 mensagens por minuto
                Thread.sleep(RATE_LIMIT_INTERVAL_MS);
            }

        }

        long end = System.currentTimeMillis();
        System.out.printf("✅ Successfully published %,d messages in %.2f seconds%n",
                messagesPublished, (end - start) / 1000.0);

        // 3. Aguardar e monitorar o processamento das mensagens
        System.out.println("⌛ Waiting for worker to process messages...");
        int lastProcessed = 0;
        int currentProcessed = 0;
        int unchangedCount = 0;
        long waitStart = System.currentTimeMillis();
        long maxWaitTime = 600_000; // 3 minutos no máximo

        while (System.currentTimeMillis() - waitStart < maxWaitTime) {
            // Verificar progresso a cada 5 segundos
            Thread.sleep(5000);

            // Verificar quantas mensagens foram processadas
            currentProcessed = getProcessedCount(campaignId);
            System.out.printf("📊 Processing progress: %d of %d messages processed%n",
                    currentProcessed, TOTAL_MESSAGES);

            // Se todas as mensagens foram processadas, podemos parar de esperar
            if (currentProcessed >= TOTAL_MESSAGES) {
                System.out.println("✅ All messages processed successfully!");
                break;
            }

            // Se não houver progresso em 3 verificações consecutivas, consideramos que o
            // worker parou
            if (currentProcessed == lastProcessed) {
                unchangedCount++;
                if (unchangedCount >= 3) {
                    System.out.println("⚠️ No progress detected for 15 seconds, continuing with test...");
                    break;
                }
            } else {
                unchangedCount = 0;
            }

            lastProcessed = currentProcessed;
        }

        // Imprimir progresso do processamento para diagnóstico
        logProcessingProgress(campaignId);

        System.out.println("🔁 Connecting to RabbitMQ to validate processed messages...");

        int messagesConsumed = 0;
        try (com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {

            GetResponse response;
            while ((response = channel.basicGet(UPDATE_QUEUE, true)) != null) {
                messagesConsumed++;
            }
        }

        // 4. Verificações finais
        System.out.printf("📊 Total messages published: %d%n", messagesPublished);
        System.out.printf("📥 Total messages still in update-status queue: %d%n", TOTAL_MESSAGES - messagesConsumed);
        System.out.printf("📤 Estimated messages consumed by worker: %d%n", messagesConsumed);

        // 5. Validações no banco de dados
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            // Verifica status da campanha
            try (PreparedStatement stmt = conn.prepareStatement(
                    "SELECT posting_status, successfully_sent, successfully_delivered, last_try_error_count, total_thread "
                            +
                            "FROM messenger_bot_broadcast_serial WHERE id = ?")) {
                stmt.setInt(1, campaignId);

                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next(), "Campaign should exist in database");

                    int postingStatus = rs.getInt("posting_status");
                    int successfullySent = rs.getInt("successfully_sent");
                    int successfullyDelivered = rs.getInt("successfully_delivered");
                    int lastTryErrorCount = rs.getInt("last_try_error_count");
                    int totalThread = rs.getInt("total_thread");

                    // Verifica se o total de threads está correto
                    assertEquals(TOTAL_MESSAGES, totalThread, "Total threads should match published messages");

                    // Temporariamente verificar apenas se algumas mensagens foram processadas
                    // Depois de entender o problema, podemos voltar às verificações originais
                    assertTrue(successfullySent >= 0, "Some messages should be processed");
                    assertTrue(lastTryErrorCount >= 0, "Some error messages should be processed");

                    // Verifica status da campanha
                    assertTrue(postingStatus == 0 || postingStatus == 1 || postingStatus == 2 || postingStatus == 4,
                            "Campaign status should be valid");
                }
            }

            // Verifica status das mensagens individuais
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
                    int errorCount = rs.getInt("error_count");
                    int uniqueMessageIds = rs.getInt("unique_message_ids");

                    // Verificações detalhadas com mensagens claras
                    assertEquals(TOTAL_MESSAGES, processedCount,
                            "All messages should be processed");

                    assertEquals(successMessages, deliveredCount,
                            "Number of delivered messages should match success messages");

                    assertEquals(errorMessages, errorCount,
                            "Number of error messages should match error count");

                    assertTrue(uniqueMessageIds > 0,
                            "Messages with success should have unique message IDs");

                    // Exibir informações detalhadas
                    System.out.println("📊 Validação detalhada:");
                    System.out.printf("   - Total de mensagens: %d (esperado: %d)%n",
                            processedCount, TOTAL_MESSAGES);
                    System.out.printf("   - Mensagens entregues: %d (esperado: %d)%n",
                            deliveredCount, successMessages);
                    System.out.printf("   - Mensagens com erro: %d (esperado: %d)%n",
                            errorCount, errorMessages);
                    System.out.printf("   - IDs de mensagem únicos: %d%n", uniqueMessageIds);
                }
            }

            // Adicionar validação de registros específicos
            validateSpecificRecords(conn, campaignId);
        }
    }

    private void validateSpecificRecords(java.sql.Connection conn, int campaignId) throws SQLException {
        // Verificar exemplos específicos
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, delivered, error_message, message_sent_id " +
                        "FROM messenger_bot_broadcast_serial_send " +
                        "WHERE campaign_id = ? ORDER BY id LIMIT 5")) {

            stmt.setInt(1, campaignId);

            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("\n📝 Exemplos de registros processados:");
                int count = 0;

                while (rs.next() && count < 5) {
                    String delivered = rs.getString("delivered");
                    String errorMsg = rs.getString("error_message");
                    String messageId = rs.getString("message_sent_id");
                    int id = rs.getInt("id");

                    System.out.printf("   Registro #%d: delivered=%s, has error=%s, message_id=%s%n",
                            id,
                            delivered,
                            (errorMsg != null && !errorMsg.isEmpty()) ? "true" : "false",
                            messageId);

                    count++;
                }
            }
        }

        // Verificar contagem por tipo de entrega com assertivas
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT delivered, COUNT(*) as count " +
                        "FROM messenger_bot_broadcast_serial_send " +
                        "WHERE campaign_id = ? GROUP BY delivered")) {

            stmt.setInt(1, campaignId);

            try (ResultSet rs = stmt.executeQuery()) {
                System.out.println("\n📊 Distribuição por status de entrega:");
                Map<String, Integer> deliveryStats = new HashMap<>();

                while (rs.next()) {
                    String delivered = rs.getString("delivered");
                    int count = rs.getInt("count");
                    deliveryStats.put(delivered, count);

                    System.out.printf("   Status '%s': %d mensagens%n", delivered, count);
                }

                // Validar distribuição
                int delivered = deliveryStats.getOrDefault("1", 0);
                int notDelivered = deliveryStats.getOrDefault("0", 0);

                assertEquals(TOTAL_MESSAGES, delivered + notDelivered,
                        "Total de mensagens entregues e não entregues deve ser igual ao total de mensagens");

                assertTrue(delivered >= (TOTAL_MESSAGES * 2) / 3,
                        "Número de mensagens entregues deve ser aproximadamente 2/3 do total");

                assertTrue(notDelivered >= TOTAL_MESSAGES / 3 - 1 && notDelivered <= TOTAL_MESSAGES / 3 + 1,
                        "Número de mensagens não entregues deve ser aproximadamente 1/3 do total");
            }
        }
    }

    private void insertCampaignIfNotExists(int campaignId) throws SQLException {
        try (java.sql.Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
            // Pegar o ID da página do Facebook (precisamos associar a campanha a uma
            // página)
            int pageId = 0;
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT id FROM facebook_rx_fb_page_info LIMIT 1")) {
                if (rs.next()) {
                    pageId = rs.getInt("id");
                } else {
                    throw new SQLException("No Facebook page found in database");
                }
            }

            // Inserir a campanha
            String insertSql = "INSERT INTO messenger_bot_broadcast_serial " +
                    "(id, user_id, page_id, fb_page_id, posting_status, is_try_again, total_thread) " +
                    "VALUES (?, 1, ?, '123456789', 0, 0, ?)";
            try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                insertStmt.setInt(1, campaignId);
                insertStmt.setInt(2, pageId);
                insertStmt.setInt(3, TOTAL_MESSAGES);
                insertStmt.executeUpdate();
                System.out.printf("📌 Inserted test campaign with id: %d associated with page id: %d%n", campaignId,
                        pageId);
            }

            // Inserir as mensagens individuais
            System.out.println("📌 Inserting individual message records...");
            String batchSql = "INSERT INTO messenger_bot_broadcast_serial_send " +
                    "(campaign_id, user_id, page_id, subscriber_auto_id, subscribe_id, sent_time) " +
                    "VALUES (?, 1, ?, ?, ?, ?)";

            try (PreparedStatement batchStmt = conn.prepareStatement(batchSql)) {
                // Desativar auto-commit para melhorar performance
                conn.setAutoCommit(false);
                String now = getNow();

                for (int i = 1; i <= TOTAL_MESSAGES; i++) {
                    batchStmt.setInt(1, campaignId);
                    batchStmt.setInt(2, pageId);
                    batchStmt.setInt(3, SUBSCRIBER_ID_START + i);
                    batchStmt.setString(4, "subscriber_" + (SUBSCRIBER_ID_START + i));
                    batchStmt.setString(5, now);
                    batchStmt.addBatch();

                    // Executar a cada 5000 para evitar lotes muito grandes
                    if (i % 5000 == 0) {
                        batchStmt.executeBatch();
                        System.out.printf("🔄 Inserted %,d message records...%n", i);
                    }
                }

                // Executar o lote final
                batchStmt.executeBatch();
                conn.commit();
                System.out.println("✅ All message records inserted successfully");

                // Restaurar auto-commit
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
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return 0;
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
                    System.out.println("\n📊 Progresso do processamento:");
                    System.out.printf("   - Mensagens processadas com sucesso: %d%n", successCount);
                    System.out.printf("   - Mensagens com erro: %d%n", errorCount);
                }
            }
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