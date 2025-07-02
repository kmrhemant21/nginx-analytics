from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import os

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Logs_Kafka_To_PostgreSQL") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# NGINX log schema
nginx_schema = StructType() \
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

# Kafka connection details
kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
kafka_topic = os.environ.get("KAFKA_TOPIC", "nginx-logs")

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON from Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), nginx_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", expr("to_timestamp(time_iso8601)")) \
    .withColumn("endpoint", expr("regexp_extract(request_uri, '^(/[^/]+/[^/]+/[^/]+)', 1)"))

# Add processing timestamp
enriched_df = parsed_df.withColumn("processing_time", current_timestamp())

# Function to write to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    
    try:
        # Cast numeric fields to appropriate types
        batch_df = batch_df.withColumn("request_length", col("request_length").cast("integer")) \
            .withColumn("status", col("status").cast("integer")) \
            .withColumn("body_bytes_sent", col("body_bytes_sent").cast("integer")) \
            .withColumn("bytes_sent", col("bytes_sent").cast("integer")) \
            .withColumn("request_time", col("request_time").cast("double")) \
            .withColumn("upstream_response_time", col("upstream_response_time").cast("double")) \
            .withColumn("upstream_response_length", col("upstream_response_length").cast("integer"))
        
        # Select and write to PostgreSQL
        batch_df.select(
            "timestamp", "msec", "connection", "connection_requests", "pid", 
            "request_id", "request_length", "remote_addr", "remote_port", 
            "time_local", "time_iso8601", "request", "request_uri", "args", 
            "status", "body_bytes_sent", "bytes_sent", "http_referer", 
            "http_user_agent", "http_host", "server_name", "request_time", 
            "upstream", "upstream_response_time", "upstream_response_length", 
            "ssl_protocol", "scheme", "request_method", "geoip2_country_code", 
            "endpoint"
        ).write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
            .option("dbtable", "raw_logs") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"Batch {batch_id}: ERROR writing to PostgreSQL: {e}")

# Write to PostgreSQL
query = enriched_df \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/opt/bitnami/spark/work-dir/checkpoints/nginx_to_postgres") \
    .trigger(processingTime="10 seconds") \
    .start()

# Wait for termination
query.awaitTermination()