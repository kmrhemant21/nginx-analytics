-- Create database for NGINX logs
CREATE DATABASE IF NOT EXISTS nginx_logs;

-- Raw logs table
CREATE TABLE IF NOT EXISTS nginx_logs.raw_logs
(
    timestamp DateTime,
    msec String,
    connection String,
    connection_requests String,
    pid String,
    request_id String,
    request_length UInt32,
    remote_addr String,
    remote_port String,
    time_local String,
    time_iso8601 String,
    request String,
    request_uri String,
    args String,
    status UInt16,
    body_bytes_sent UInt32,
    bytes_sent UInt32,
    http_referer String,
    http_user_agent String,
    http_host String,
    server_name String,
    request_time Float64,
    upstream String,
    upstream_response_time Float64,
    upstream_response_length UInt32,
    ssl_protocol String,
    scheme String,
    request_method String,
    geoip2_country_code String,
    endpoint String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, endpoint, request_method)
TTL timestamp + INTERVAL 90 DAY;

-- Anomalies table
CREATE TABLE IF NOT EXISTS nginx_logs.anomalies
(
    timestamp DateTime,
    detection_timestamp DateTime DEFAULT now(),
    endpoint String,
    request_uri String,
    remote_addr String,
    status UInt16,
    request_time Float64,
    anomaly_score Float64,
    model_version String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, endpoint)
TTL timestamp + INTERVAL 90 DAY;

-- Model metrics table
CREATE TABLE IF NOT EXISTS nginx_logs.model_metrics
(
    run_id String,
    model_version String,
    training_timestamp DateTime DEFAULT now(),
    metrics_json String,
    parameters_json String,
    records_count UInt32,
    accuracy Float64,
    precision Float64,
    recall Float64,
    f1_score Float64
)
ENGINE = MergeTree()
ORDER BY (training_timestamp, model_version);