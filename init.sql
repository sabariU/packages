CREATE DATABASE IF NOT EXISTS etl_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE etl_demo;

CREATE TABLE IF NOT EXISTS users (
  user_id     INT PRIMARY KEY,
  first_name  VARCHAR(80) NOT NULL,
  last_name   VARCHAR(80) NOT NULL,
  email       VARCHAR(255) NOT NULL UNIQUE,
  created_at  DATETIME NOT NULL,
  loaded_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_created_at (created_at)
);
