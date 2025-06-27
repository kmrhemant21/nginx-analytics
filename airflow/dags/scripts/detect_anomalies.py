from pyspark.sql import SparkSession
import mlflow.pyfunc
from pyspark.sql.functions import col, struct, udf
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructType, StructField
import os
from datetime import datetime

# Set up MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minioadmin"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minioadmin"

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Detect_Anomalies") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.3.2-patch9") \
    .getOrCreate()

def detect_anomalies():
    # Load the production model
    model = mlflow.pyfunc.load_model("models:/nginx_anomaly_detector/Production")
    model_version = model.metadata.get_model_info().version
    
    # Extract logs from the last hour
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
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    """
    
    # Load data from ClickHouse
    logs_df = spark.read \
        .format("jdbc") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/nginx_logs") \
        .option("query", query) \
        .option("user", "default") \
        .option("password", "") \
        .load()
    
    # Handle missing values
    logs_df = logs_df.na.fill({
        "request_length": 0,
        "body_bytes_sent": 0,
        "bytes_sent": 0,
        "request_time": 0,
        "upstream_response_time": 0,
        "geoip2_country_code": "UNKNOWN",
        "endpoint": "UNKNOWN",
        "request_method": "UNKNOWN"
    })
    
    # Define UDF for model prediction
    def predict_anomaly(features):
        try:
            # Convert to format expected by model
            input_features = {
                "endpoint": features["endpoint"],
                "request_method": features["request_method"],
                "geoip2_country_code": features["geoip2_country_code"],
                "status_code": features["status_code"],
                "request_length": features["request_length"],
                "body_bytes_sent": features["body_bytes_sent"],
                "bytes_sent": features["bytes_sent"],
                "hour_of_day": features["hour_of_day"],
                "day_of_week": features["day_of_week"],
                "request_time": features["request_time"]
            }
            
            # Make prediction
            result = model.predict([input_features])[0]
            return (bool(result), float(0.95))  # Dummy score for now
        except Exception as e:
            print(f"Prediction error: {e}")
            return (False, 0.0)
    
    # Register UDF
    predict_schema = StructType([
        StructField("is_anomaly", BooleanType(), False),
        StructField("anomaly_score", DoubleType(), False)
    ])
    predict_udf = udf(predict_anomaly, predict_schema)
    
    # Apply prediction
    feature_cols = ["endpoint", "request_method", "geoip2_country_code", "status_code", 
                   "request_length", "body_bytes_sent", "bytes_sent", "hour_of_day", 
                   "day_of_week", "request_time"]
    
    prediction_df = logs_df.withColumn(
        "prediction", 
        predict_udf(struct(*[col(c) for c in feature_cols]))
    ).withColumn(
        "is_anomaly", 
        col("prediction.is_anomaly")
    ).withColumn(
        "anomaly_score", 
        col("prediction.anomaly_score")
    ).withColumn(
        "model_version", 
        lit(model_version)
    )
    
    # Extract anomalies
    anomalies_df = prediction_df.filter(col("is_anomaly") == True)
    
    # Save anomalies for next step
    if not anomalies_df.isEmpty():
        anomalies_df.write \
            .mode("overwrite") \
            .parquet("/tmp/nginx_anomalies")
    
    # Return anomaly count
    anomaly_count = anomalies_df.count()
    print(f"Detected {anomaly_count} anomalies")
    
    return {
        "anomaly_count": anomaly_count,
        "processed_records": logs_df.count(),
        "model_version": model_version
    }

if __name__ == "__main__":
    result = detect_anomalies()
    print(f"Anomaly detection complete: {result}")
    spark.stop()