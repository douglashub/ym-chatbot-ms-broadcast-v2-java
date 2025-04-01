-- Criar banco e garantir acesso total ao usuário
CREATE DATABASE IF NOT EXISTS `ym-chatbot`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'dbuser'@'%' IDENTIFIED BY 'dbpassword';
GRANT ALL PRIVILEGES ON *.* TO 'dbuser'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

USE `ym-chatbot`;

-- 1. Drop tables in an order that respects foreign key constraints
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial_logger_serial`;
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial_send`;
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial_request`;
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial_logger`;
DROP TABLE IF EXISTS `messenger_bot_subscriber`;
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial`;
DROP TABLE IF EXISTS `facebook_rx_fb_page_info`;

-- 2. Create tables

-- Facebook page info table
CREATE TABLE IF NOT EXISTS `facebook_rx_fb_page_info` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` VARCHAR(200) NOT NULL,
  `page_name` VARCHAR(200) NULL,
  `page_access_token` TEXT NOT NULL,
  PRIMARY KEY (`id`),
  KEY `page_id` (`page_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Campaign table
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial` (
  `id` INT NOT NULL PRIMARY KEY,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` INT NOT NULL DEFAULT 1,
  `fb_page_id` VARCHAR(200) NOT NULL DEFAULT '',
  `message` MEDIUMTEXT,
  `posting_status` ENUM('0','1','2','3','4') NOT NULL DEFAULT '0',
  `is_try_again` ENUM('0','1') NOT NULL DEFAULT '1',
  `last_try_error_count` INT NOT NULL DEFAULT 0,
  `total_thread` INT NOT NULL DEFAULT 0,
  `successfully_sent` INT NOT NULL DEFAULT 0,
  `successfully_delivered` INT NOT NULL DEFAULT 0,
  `schedule_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `completed_at` DATETIME DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Request table for broadcast serial
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial_request` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `campaign_id` INT NOT NULL,
  `request_url` VARCHAR(255) NOT NULL,
  `request_data` TEXT NOT NULL,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY `idx_campaign_id` (`campaign_id`),
  FOREIGN KEY (`campaign_id`) REFERENCES `messenger_bot_broadcast_serial`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Subscriber table
CREATE TABLE IF NOT EXISTS `messenger_bot_subscriber` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `last_error_message` TEXT,
  `unavailable` TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Messages table
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial_send` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `campaign_id` INT NOT NULL,
  `request_id` INT DEFAULT NULL,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` INT NOT NULL DEFAULT 1,
  `messenger_bot_subscriber` INT NOT NULL DEFAULT 0,
  `subscriber_auto_id` INT NOT NULL,
  `subscribe_id` VARCHAR(255) NOT NULL DEFAULT '',
  `subscriber_name` VARCHAR(255) NOT NULL DEFAULT '',
  `subscriber_lastname` VARCHAR(200) NOT NULL DEFAULT '',
  `processed` ENUM('0','1') NOT NULL DEFAULT '0',
  `delivered` ENUM('0','1') NOT NULL DEFAULT '0',
  `delivery_time` DATETIME DEFAULT NULL,
  `error_message` TEXT,
  `message_sent_id` VARCHAR(255) NOT NULL DEFAULT '',
  `sent_time` DATETIME DEFAULT NULL,
  `processed_by` VARCHAR(30) DEFAULT NULL,
  KEY `idx_campaign_processed` (`campaign_id`, `processed`),
  FOREIGN KEY (`campaign_id`) REFERENCES `messenger_bot_broadcast_serial`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`request_id`) REFERENCES `messenger_bot_broadcast_serial_request`(`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Broadcast serial logger table
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial_logger` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` INT NOT NULL DEFAULT 1,
  `status` TINYINT NOT NULL DEFAULT 2 COMMENT '2=pending, 5=sending',
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Broadcast serial logger serial table
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial_logger_serial` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `logger_id` INT NOT NULL,
  `serial_id` INT NOT NULL,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`logger_id`) REFERENCES `messenger_bot_broadcast_serial_logger`(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`serial_id`) REFERENCES `messenger_bot_broadcast_serial`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. Insert initial data
INSERT INTO `facebook_rx_fb_page_info` (`user_id`, `page_id`, `page_name`, `page_access_token`) 
VALUES (1, '123456789', 'Test Page', 'EAABODYFiZBWwBANcZAgHD6k...(token simulado)');