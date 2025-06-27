from pyspark.sql import SparkSession
import os
from datetime import datetime

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Store_Anomalies") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.3.2-patch9") \
    .getOrCreate()

def store_anomalies():
    # Check if anomalies file exists
    anomalies_path = "/tmp/nginx_anomalies"
    
    try:
        # Load anomalies
        anomalies_df = spark.read.parquet(anomalies_path)
        
        if anomalies_df.isEmpty():
            print("No anomalies to store")
            return {"anomalies_stored": 0}
        
        # Select relevant columns for storage
        anomalies_to_store = anomalies_df.select(
            "timestamp",
            "endpoint",
            "request_uri",
            "remote_addr",
            "status_code",
            "request_time",
            "anomaly_score",
            "model_version"
        )
        
        # Write to ClickHouse
        anomalies_to_store.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/nginx_logs") \
            .option("dbtable", "anomalies") \
            .option("user", "default") \
            .option("password", "") \
            .mode("append") \
            .save()
        
        anomaly_count = anomalies_to_store.count()
        print(f"Stored {anomaly_count} anomalies in ClickHouse")
        
        return {"anomalies_stored": anomaly_count}
    
    except Exception as e:
        print(f"Error storing anomalies: {e}")
        return {"anomalies_stored": 0, "error": str(e)}

if __name__ == "__main__":
    result = store_anomalies()
    print(f"Storage complete: {result}")
    spark.stop()