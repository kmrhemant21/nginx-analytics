#!/bin/bash
clickhouse-server &
SERVER_PID=$!

# Wait for ClickHouse server to start
until clickhouse-client --query "SELECT 1"; do
    sleep 1
done

# Run your init.sql
clickhouse-client < /etc/clickhouse-server/config.d/init.sql

wait $SERVER_PID
