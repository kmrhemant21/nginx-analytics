#!/bin/bash

# Wait for Airflow web server to be available
echo "Waiting for Airflow webserver..."
until curl --output /dev/null --silent --head --fail http://airflow-webserver:8080; do
    printf '.'
    sleep 5
done

# Add connections
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077' \
    --conn-extra '{"queue": "default"}'

airflow connections add 'clickhouse_default' \
    --conn-type 'jdbc' \
    --conn-host 'clickhouse' \
    --conn-port '8123' \
    --conn-schema 'nginx_logs' \
    --conn-extra '{"driver": "com.clickhouse.jdbc.ClickHouseDriver", "url": "jdbc:clickhouse://clickhouse:8123/nginx_logs"}'

airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-port '5432' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-schema 'airflow'

airflow connections add 'mlflow_default' \
    --conn-type 'http' \
    --conn-host 'mlflow' \
    --conn-port '5000' \
    --conn-schema 'http'

echo "Connections added successfully"