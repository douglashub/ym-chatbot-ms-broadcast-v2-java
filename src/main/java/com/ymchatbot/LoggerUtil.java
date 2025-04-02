package com.ymchatbot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.Arrays;

/**
 * Utility class for logging using both SLF4J and file-based logging
 */
public class LoggerUtil {
    private static final Logger logger = LoggerFactory.getLogger(LoggerUtil.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Log a debug message to both SLF4J and a file
     * 
     * @param message The message to log
     */
    public static void debug(String message) {
        logger.debug(message);
        logToFile("DEBUG", message);
    }

    /**
     * Log an error message to both SLF4J and a file
     * 
     * @param message The message to log
     */
    public static void error(String message) {
        logger.error(message);
        logToFile("ERROR", message);
    }

    /**
     * Log an error message with an exception to both SLF4J and a file
     * 
     * @param message   The message to log
     * @param throwable The exception to log
     */
    public static void error(String message, Throwable throwable) {
        logger.error(message, throwable);
        
        try {
            ObjectNode errorNode = objectMapper.createObjectNode()
                .put("message", message)
                .put("errorType", throwable.getClass().getName())
                .put("errorMessage", throwable.getMessage())
                .put("stackTrace", Arrays.toString(throwable.getStackTrace()));

            logToFile("ERROR", objectMapper.writeValueAsString(errorNode));
        } catch (Exception e) {
            logger.error("Erro ao serializar exceção", e);
            logToFile("ERROR", message + " - " + throwable.getMessage());
        }
    }

    /**
     * Log an info message to both SLF4J and a file
     * 
     * @param message The message to log
     */
    public static void info(String message) {
        logger.info(message);
        logToFile("INFO", message);
    }

    /**
     * Log a warning message to both SLF4J and a file
     * 
     * @param message The message to log
     */
    public static void warn(String message) {
        logger.warn(message);
        logToFile("WARN", message);
    }

    /**
     * Write a log entry to a file
     * 
     * @param level   The log level
     * @param message The message to log
     */
    private static synchronized void logToFile(String level, String message) {
        try {
            String today = LocalDate.now().format(DATE_FORMATTER);
            String logFile = "../storage/logs/worker_send_message_" + today + ".log";

            // Create parent directories if they don't exist
            Files.createDirectories(Paths.get("../storage/logs"));

            // Criar estrutura JSON do log usando ObjectMapper
            ObjectNode logNode = objectMapper.createObjectNode()
                .put("timestamp", LocalDateTime.now().format(TIMESTAMP_FORMATTER))
                .put("level", level)
                .put("message", message);

            String logEntry = objectMapper.writeValueAsString(logNode) + System.lineSeparator();

            Files.write(
                    Paths.get(logFile),
                    logEntry.getBytes(StandardCharsets.UTF_8),
                    Files.exists(Paths.get(logFile)) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
        } catch (IOException e) {
            // Can't use the logger here to avoid infinite recursion
            System.err.println("Failed to write to log file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Check if debug logging is enabled
     * 
     * @return true if debug is enabled, false otherwise
     */
    public static boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * Clean up old log files (older than specified days)
     * 
     * @param daysToKeep Number of days of logs to keep
     */
    public static void cleanupOldLogs(int daysToKeep) {
        try {
            LocalDate cutoffDate = LocalDate.now().minusDays(daysToKeep);
            Files.list(Paths.get("../storage/logs"))
                    .filter(path -> {
                        String filename = path.getFileName().toString();
                        if (!filename.startsWith("worker_send_message_")) {
                            return false;
                        }

                        try {
                            String dateStr = filename.substring("worker_send_message_".length(),
                                    filename.lastIndexOf('.'));
                            LocalDate fileDate = LocalDate.parse(dateStr, DATE_FORMATTER);
                            return fileDate.isBefore(cutoffDate);
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            logger.info("Deleted old log file: " + path.getFileName());
                        } catch (IOException e) {
                            logger.error("Failed to delete old log file: " + path.getFileName(), e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Error cleaning up old log files", e);
        }
    }
}