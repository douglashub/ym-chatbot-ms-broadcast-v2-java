-- Criar banco e garantir acesso total ao usuário
CREATE DATABASE IF NOT EXISTS `ym-chatbot`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'dbuser'@'%' IDENTIFIED BY 'dbpassword';
GRANT ALL PRIVILEGES ON *.* TO 'dbuser'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

USE `ym-chatbot`;

-- 1. Drop old tables (opcional para garantir recriação)
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial_send`;
DROP TABLE IF EXISTS `messenger_bot_broadcast_serial`;
DROP TABLE IF EXISTS `facebook_rx_fb_page_info`;
DROP TABLE IF EXISTS `messenger_bot_subscriber`;

-- 2. Create necessary tables
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

-- Campaign table (atualizada para incluir schedule_time)
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial` (
  `id` INT NOT NULL PRIMARY KEY,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` INT NOT NULL DEFAULT 1,
  `fb_page_id` VARCHAR(200) NOT NULL DEFAULT '',
  `posting_status` TINYINT NOT NULL DEFAULT 0 COMMENT '0=pending, 1=processing, 2=completed, 3=paused, 4=error',
  `is_try_again` TINYINT NOT NULL DEFAULT 0,
  `successfully_sent` INT NOT NULL DEFAULT 0,
  `successfully_delivered` INT NOT NULL DEFAULT 0,
  `last_try_error_count` INT NOT NULL DEFAULT 0,
  `total_thread` INT NOT NULL DEFAULT 0,
  `schedule_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `completed_at` DATETIME DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Messages table
CREATE TABLE IF NOT EXISTS `messenger_bot_broadcast_serial_send` (
  `id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `campaign_id` INT NOT NULL,
  `user_id` INT NOT NULL DEFAULT 1,
  `page_id` INT NOT NULL DEFAULT 1,
  `subscriber_auto_id` INT NOT NULL,
  `subscribe_id` VARCHAR(255) NOT NULL DEFAULT '',
  `delivered` ENUM('0','1') NOT NULL DEFAULT '0',
  `delivery_time` DATETIME DEFAULT NULL,
  `processed` ENUM('0','1') NOT NULL DEFAULT '0',
  `error_message` TEXT,
  `message_sent_id` VARCHAR(255) NOT NULL DEFAULT '',
  `sent_time` DATETIME DEFAULT NULL,
  `processed_by` VARCHAR(30) DEFAULT NULL,
  KEY `idx_campaign_processed` (`campaign_id`, `processed`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Minimal table for subscribers (para evitar erros)
CREATE TABLE IF NOT EXISTS `messenger_bot_subscriber` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `last_error_message` TEXT,
  `unavailable` TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Insert some initial data
INSERT INTO `facebook_rx_fb_page_info` (`user_id`, `page_id`, `page_name`, `page_access_token`) 
VALUES (1, '123456789', 'Test Page', 'EAABODYFiZBWwBANcZAgHD6k...(token simulado)');
