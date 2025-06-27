from pyspark.sql import SparkSession
import mlflow
import os

# Set up MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minioadmin"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minioadmin"

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Extract_Training_Data") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.3.2-patch9,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Extract training data from ClickHouse
def extract_training_data():
    # Start MLflow run
    with mlflow.start_run(run_name="extract_training_data") as run:
        # Extract logs from the last 7 days
        query = """
        SELECT
            timestamp,
            CAST(request_length AS INT) as request_length,
            remote_addr,
            request_uri,
            endpoint,
            CAST(status AS INT) as status_code,
            CAST(body_bytes_sent AS INT) as body_bytes_sent,
            CAST(bytes_sent AS INT) as bytes_sent,
            http_user_agent,
            http_host,
            CAST(request_time AS DOUBLE) as request_time,
            CAST(upstream_response_time AS DOUBLE) as upstream_response_time,
            request_method,
            geoip2_country_code,
            toHour(timestamp) as hour_of_day,
            toDayOfWeek(timestamp) as day_of_week
        FROM nginx_logs.raw_logs
        WHERE timestamp >= now() - INTERVAL 7 DAY
        """
        
        # Load data from ClickHouse
        training_data = spark.read \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/nginx_logs") \
            .option("query", query) \
            .option("user", "default") \
            .option("password", "") \
            .load()
        
        # Log dataset info
        record_count = training_data.count()
        mlflow.log_param("training_records", record_count)
        mlflow.log_param("start_date", training_data.agg({"timestamp": "min"}).collect()[0][0])
        mlflow.log_param("end_date", training_data.agg({"timestamp": "max"}).collect()[0][0])
        
        print(f"Extracted {record_count} records for training")
        
        # Save training data for the model training step
        training_data.write \
            .mode("overwrite") \
            .parquet("/tmp/nginx_training_data")
        
        return {
            "run_id": run.info.run_id,
            "record_count": record_count
        }

if __name__ == "__main__":
    result = extract_training_data()
    print(f"Extraction complete: {result}")
    spark.stop()