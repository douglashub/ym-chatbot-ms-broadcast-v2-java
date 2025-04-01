package com.ymchatbot;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class FileRateLimiter {
    private static final String RATE_LIMIT_DIR = "../storage/rate-limit/";
    
    public static boolean allow(String timePattern, int limit) {
        try {
            String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern(timePattern));
            Path path = Paths.get(RATE_LIMIT_DIR + now + ".log");
            
            if (!Files.exists(path)) {
                Files.createDirectories(path.getParent());
                Files.write(path, List.of(String.valueOf(System.currentTimeMillis())));
                return true;
            }

            List<String> lines = Files.readAllLines(path).stream()
                    .filter(s -> !s.isBlank())
                    .collect(Collectors.toList());

            if (lines.size() >= limit) {
                return false;
            }

            Files.write(path, List.of(String.valueOf(System.currentTimeMillis())), StandardOpenOption.APPEND);
            return true;
        } catch (IOException e) {
            LoggerUtil.error("FileRateLimiter error", e);
            return true; // fallback: never block due to IO errors
        }
    }
}