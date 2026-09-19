-- Initialize raw_data schema and customers table
CREATE DATABASE raw_data;

CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.customers (
 id SERIAL PRIMARY KEY,
 name TEXT NOT NULL,
 email TEXT,
 created_at TIMESTAMP DEFAULT now()
);

-- Insert sample rows
INSERT INTO raw_data.customers (name, email) VALUES
('Alice','alice@example.com'),
('Bob','bob@example.com'),
('Charlie','charlie@example.com');
