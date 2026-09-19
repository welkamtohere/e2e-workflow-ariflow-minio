-- Initialize target_db and dim_customers table
CREATE DATABASE IF NOT EXISTS target_db;
USE target_db;

CREATE TABLE IF NOT EXISTS dim_customers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 name VARCHAR(255) NOT NULL,
 email VARCHAR(255),
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample rows
INSERT INTO dim_customers (name, email) VALUES
('Alice','alice@example.com'),
('Bob','bob@example.com');


