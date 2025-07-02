# Start all services
docker-compose up -d

# Initialize Airflow (first time only)
docker exec airflow-webserver airflow db init
docker exec airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Setup Airflow connections
docker exec airflow-webserver bash /opt/airflow/config/connections.sh

# Start real-time ingestion
docker exec -d spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.clickhouse:clickhouse-jdbc:0.3.2-patch9 \
    --driver-memory 1g \
    --executor-memory 1g \
    /opt/bitnami/spark/work-dir/real-time-ingestion.py