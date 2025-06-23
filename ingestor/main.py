from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
import os

# Complete NGINX log schema
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

# Create Spark session
spark = SparkSession.builder \
    .appName("NginxLogsIngestion") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BROKER", "kafka:9092")) \
    .option("subscribe", os.environ.get("KAFKA_NGINX_TOPIC", "nginx")) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON and filter out monitoring agents
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .filter(col("http_user_agent") != "promtail/2.2.1") \
    .withColumn("timestamp", to_timestamp(col("time_iso8601"))) \
    .withColumn("request_time_seconds", col("request_time").cast(DoubleType())) \
    .withColumn("status_code", col("status").cast(IntegerType()))

# Debug - print schema
parsed_df.printSchema()

# Extract API endpoint from request_uri
from pyspark.sql.functions import regexp_extract
parsed_df = parsed_df.withColumn(
    "endpoint", 
    regexp_extract(col("request_uri"), "^/[^/]+/([^/]+)", 1)
)

# Write to PostgreSQL
def write_batch_to_postgres(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        # Write raw logs
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
            .option("dbtable", "raw_logs") \
            .option("user", "superset") \
            .option("password", "superset") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id}: Wrote {batch_df.count()} records to PostgreSQL")

# Start the streaming query
query = parsed_df.writeStream \
    .foreachBatch(write_batch_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/ingestor") \
    .start()

# Wait for the query to terminate
query.awaitTermination()