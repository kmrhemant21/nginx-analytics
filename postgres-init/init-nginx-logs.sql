-- Create database for NGINX logs
CREATE DATABASE nginx_logs;
\c nginx_logs;

-- Raw logs table
CREATE TABLE IF NOT EXISTS raw_logs (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    msec VARCHAR(255),
    connection VARCHAR(255),
    connection_requests VARCHAR(255),
    pid VARCHAR(255),
    request_id VARCHAR(255),
    request_length INTEGER,
    remote_addr VARCHAR(255),
    remote_port VARCHAR(255),
    time_local VARCHAR(255),
    time_iso8601 VARCHAR(255),
    request TEXT,
    request_uri TEXT,
    args TEXT,
    status SMALLINT,
    body_bytes_sent INTEGER,
    bytes_sent INTEGER,
    http_referer TEXT,
    http_user_agent TEXT,
    http_host VARCHAR(255),
    server_name VARCHAR(255),
    request_time DOUBLE PRECISION,
    upstream VARCHAR(255),
    upstream_response_time DOUBLE PRECISION,
    upstream_response_length INTEGER,
    ssl_protocol VARCHAR(255),
    scheme VARCHAR(255),
    request_method VARCHAR(255),
    geoip2_country_code VARCHAR(255),
    endpoint VARCHAR(255)
);

-- Create indexes for better query performance
CREATE INDEX idx_raw_logs_timestamp ON raw_logs (timestamp);
CREATE INDEX idx_raw_logs_endpoint ON raw_logs (endpoint);
CREATE INDEX idx_raw_logs_request_method ON raw_logs (request_method);
CREATE INDEX idx_raw_logs_status ON raw_logs (status);

-- Anomalies table
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    detection_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    endpoint VARCHAR(255),
    request_uri TEXT,
    remote_addr VARCHAR(255),
    status SMALLINT,
    request_time DOUBLE PRECISION,
    anomaly_score DOUBLE PRECISION,
    model_version VARCHAR(255)
);

-- Create indexes for anomalies table
CREATE INDEX idx_anomalies_timestamp ON anomalies (timestamp);
CREATE INDEX idx_anomalies_endpoint ON anomalies (endpoint);

-- Model metrics table
CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255),
    model_version VARCHAR(255),
    training_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metrics_json TEXT,
    parameters_json TEXT,
    records_count INTEGER,
    accuracy DOUBLE PRECISION,
    precision DOUBLE PRECISION,
    recall DOUBLE PRECISION,
    f1_score DOUBLE PRECISION
);

-- Create index for model metrics
CREATE INDEX idx_model_metrics_training_timestamp ON model_metrics (training_timestamp);
CREATE INDEX idx_model_metrics_model_version ON model_metrics (model_version);