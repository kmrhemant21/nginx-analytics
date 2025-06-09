-- Create database (optional if not already created)
-- CREATE DATABASE nginx_logs_db;

-- -- Connect to the correct database
\c nginx;

-- Table for raw NGINX logs ingested from Kafka
CREATE TABLE IF NOT EXISTS nginx (
    id SERIAL PRIMARY KEY,
    msec TEXT,
    connection_requests DOUBLE PRECISION,
    request_id TEXT,
    remote_addr TEXT,
    request TEXT,
    status INTEGER,
    request_time DOUBLE PRECISION,
    http_user_agent TEXT,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for KMeans predictions
CREATE TABLE IF NOT EXISTS nginx_kmeans_predictions (
    id SERIAL PRIMARY KEY,
    remote_addr TEXT,
    request TEXT,
    status INTEGER,
    request_time DOUBLE PRECISION,
    prediction INTEGER,
    predicted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);