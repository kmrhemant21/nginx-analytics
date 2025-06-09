# spark_kafka_to_postgres.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import os

# Define schema
schema = StructType() \
    .add("msec", StringType()) \
    .add("connection_requests", StringType()) \
    .add("request_id", StringType()) \
    .add("remote_addr", StringType()) \
    .add("request", StringType()) \
    .add("status", StringType()) \
    .add("request_time", StringType()) \
    .add("http_user_agent", StringType())

# Initialize Spark
spark = SparkSession.builder \
    .appName("KafkaToPostgresRawLogs") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Kafka source
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKER", "kafka:9092")) \
    .option("subscribe", os.getenv("KAFKA_NGINX_TOPIC", "nginx")) \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON logs
df_logs = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Sink to PostgreSQL
df_logs.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write \
        .format("jdbc") \
        .option("url", os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/nginx")) \
        .option("dbtable", "raw_logs") \
        .option("user", os.getenv("POSTGRES_USER", "superset")) \
        .option("password", os.getenv("POSTGRES_PASSWORD", "superset")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()) \
    .outputMode("append") \
    .start().awaitTermination()