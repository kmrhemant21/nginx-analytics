-- Raw logs table
CREATE TABLE IF NOT EXISTS raw_logs (
    id SERIAL PRIMARY KEY,
    msec TEXT,
    connection TEXT,
    connection_requests TEXT,
    pid TEXT,
    request_id TEXT,
    request_length TEXT,
    remote_addr TEXT,
    remote_user TEXT,
    remote_port TEXT,
    time_local TEXT,
    time_iso8601 TEXT,
    request TEXT,
    request_uri TEXT,
    args TEXT,
    status TEXT,
    body_bytes_sent TEXT,
    bytes_sent TEXT,
    http_referer TEXT,
    http_user_agent TEXT,
    http_x_forwarded_for TEXT,
    http_host TEXT,
    server_name TEXT,
    request_time TEXT,
    upstream TEXT,
    upstream_connect_time TEXT,
    upstream_header_time TEXT,
    upstream_response_time TEXT,
    upstream_response_length TEXT,
    upstream_cache_status TEXT,
    ssl_protocol TEXT,
    ssl_cipher TEXT,
    scheme TEXT,
    request_method TEXT,
    server_protocol TEXT,
    pipe TEXT,
    gzip_ratio TEXT,
    http_cf_ray TEXT,
    geoip2_country_code TEXT,
    timestamp TIMESTAMP,
    request_time_seconds DOUBLE PRECISION,
    status_code INTEGER,
    endpoint TEXT
);

-- Anomaly detection results table
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    remote_addr TEXT,
    request_uri TEXT,
    anomaly_score DOUBLE PRECISION,
    is_anomaly BOOLEAN,
    anomaly_reason TEXT,
    detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Request time predictions table
CREATE TABLE IF NOT EXISTS response_time_predictions (
    id SERIAL PRIMARY KEY,
    endpoint TEXT,
    request_method TEXT,
    predicted_time DOUBLE PRECISION,
    actual_time DOUBLE PRECISION,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Traffic forecasts table
CREATE TABLE IF NOT EXISTS traffic_forecasts (
    id SERIAL PRIMARY KEY,
    forecast_timestamp TIMESTAMP,
    endpoint TEXT,
    predicted_requests INTEGER,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_raw_logs_endpoint ON raw_logs(endpoint);
CREATE INDEX IF NOT EXISTS idx_raw_logs_remote_addr ON raw_logs(remote_addr);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp);
CREATE INDEX IF NOT EXISTS idx_response_time_endpoint ON response_time_predictions(endpoint);