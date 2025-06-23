from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, stddev, when, lit
from pyspark.sql.types import DoubleType, BooleanType, StringType, TimestampType
import os
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("NginxAnomalyDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Function to detect anomalies
def detect_anomalies(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: No data")
        return
    
    print(f"Batch {batch_id}: Processing {batch_df.count()} records")
    
    # 1. Traffic volume anomalies (per IP address)
    traffic_stats = batch_df.groupBy("remote_addr") \
        .agg(
            count("*").alias("request_count"),
            avg("request_time_seconds").alias("avg_request_time")
        )
    
    # Get historical stats from PostgreSQL
    try:
        historical_stats = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
            .option("dbtable", """
                (SELECT remote_addr, 
                        AVG(request_count) as avg_requests,
                        STDDEV(request_count) as stddev_requests
                FROM (
                    SELECT remote_addr, 
                           DATE_TRUNC('hour', timestamp) as hour,
                           COUNT(*) as request_count
                    FROM raw_logs
                    WHERE timestamp > NOW() - INTERVAL '7 days'
                    GROUP BY remote_addr, DATE_TRUNC('hour', timestamp)
                ) as hourly_stats
                GROUP BY remote_addr) as stats
            """) \
            .option("user", "superset") \
            .option("password", "superset") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # Join with current stats and detect anomalies
        anomalies = traffic_stats.join(
            historical_stats, 
            "remote_addr", 
            "left"
        ).withColumn(
            "z_score", 
            when(col("stddev_requests").isNotNull() & (col("stddev_requests") > 0), 
                 (col("request_count") - col("avg_requests")) / col("stddev_requests"))
            .otherwise(lit(0))
        ).withColumn(
            "is_anomaly", 
            (col("z_score").isNotNull() & (col("z_score").abs() > 3)) | 
            (col("avg_requests").isNull() & (col("request_count") > 100))
        ).withColumn(
            "anomaly_reason",
            when(col("z_score") > 3, "Unusually high traffic")
            .when(col("z_score") < -3, "Unusually low traffic")
            .when(col("avg_requests").isNull() & (col("request_count") > 100), "New IP with high traffic")
            .otherwise(None)
        ).filter(col("is_anomaly") == True)
        
        # If anomalies found, write to PostgreSQL
        if anomalies.count() > 0:
            # Get sample requests for each anomalous IP
            sample_requests = batch_df.join(
                anomalies.select("remote_addr"), 
                "remote_addr"
            ).select(
                "timestamp", "remote_addr", "request_uri", "status_code", "request_time_seconds"
            )
            
            # Calculate anomaly scores
            from pyspark.sql.functions import abs as sql_abs
            anomaly_details = sample_requests.join(
                anomalies.select("remote_addr", "z_score", "is_anomaly", "anomaly_reason"),
                "remote_addr"
            ).withColumn(
                "anomaly_score", 
                sql_abs(col("z_score"))
            ).select(
                "timestamp", "remote_addr", "request_uri", 
                "anomaly_score", "is_anomaly", "anomaly_reason"
            )
            
            # Write to PostgreSQL
            anomaly_details.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
                .option("dbtable", "anomalies") \
                .option("user", "superset") \
                .option("password", "superset") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"Batch {batch_id}: Found {anomalies.count()} anomalies")
    
    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")

# Create streaming DataFrame from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BROKER", "kafka:9092")) \
    .option("subscribe", os.environ.get("KAFKA_NGINX_TOPIC", "nginx")) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Define schema (same as ingestor)
schema = StructType() \
    .add("msec", StringType()) \
    .add("connection", StringType()) \
    .add("connection_requests", StringType()) \
    .add("pid", StringType()) \
    .add("request_id", StringType()) \
    .add("request_length", StringType()) \
    .add("remote_addr", StringType()) \
    .add("remote_user", StringType()) \
    .add("remote_port", StringType()) \
    .add("time_local", StringType()) \
    .add("time_iso8601", StringType()) \
    .add("request", StringType()) \
    .add("request_uri", StringType()) \
    .add("args", StringType()) \
    .add("status", StringType()) \
    .add("body_bytes_sent", StringType()) \
    .add("bytes_sent", StringType()) \
    .add("http_referer", StringType()) \
    .add("http_user_agent", StringType()) \
    .add("http_x_forwarded_for", StringType()) \
    .add("http_host", StringType()) \
    .add("server_name", StringType()) \
    .add("request_time", StringType()) \
    .add("upstream", StringType()) \
    .add("upstream_connect_time", StringType()) \
    .add("upstream_header_time", StringType()) \
    .add("upstream_response_time", StringType()) \
    .add("upstream_response_length", StringType()) \
    .add("upstream_cache_status", StringType()) \
    .add("ssl_protocol", StringType()) \
    .add("ssl_cipher", StringType()) \
    .add("scheme", StringType()) \
    .add("request_method", StringType()) \
    .add("server_protocol", StringType()) \
    .add("pipe", StringType()) \
    .add("gzip_ratio", StringType()) \
    .add("http_cf_ray", StringType()) \
    .add("geoip2_country_code", StringType())

# Parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .filter(col("http_user_agent") != "promtail/2.2.1") \
    .withColumn("timestamp", to_timestamp(col("time_iso8601"))) \
    .withColumn("request_time_seconds", col("request_time").cast(DoubleType())) \
    .withColumn("status_code", col("status").cast(IntegerType()))

# Process in micro-batches with a 1-minute window
query = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute")) \
    .applyInPandas(
        lambda pandas_df: pandas_df,
        parsed_df.schema
    ) \
    .writeStream \
    .foreachBatch(detect_anomalies) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/anomaly_detection") \
    .trigger(processingTime="1 minute") \
    .start()

query.awaitTermination()