# spark_kmeans_to_postgres.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import os

spark = SparkSession.builder \
    .appName("KMeansAnomalyDetection") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Load data from PostgreSQL
df_logs = spark.read \
    .format("jdbc") \
    .option("url", os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/nginx")) \
    .option("dbtable", "raw_logs") \
    .option("user", os.getenv("POSTGRES_USER", "admin")) \
    .option("password", os.getenv("POSTGRES_PASSWORD", "admin")) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Preprocess
df_features = df_logs \
    .withColumn("connection_requests", col("connection_requests").cast(DoubleType())) \
    .withColumn("status", col("status").cast(DoubleType())) \
    .withColumn("request_time", col("request_time").cast(DoubleType())) \
    .na.drop(subset=["connection_requests", "status", "request_time"])

# Assemble features
assembler = VectorAssembler(
    inputCols=["connection_requests", "status", "request_time"],
    outputCol="features"
)
df_vectorized = assembler.transform(df_features)

# Train KMeans model
model = KMeans(k=2, seed=42).fit(df_vectorized)

# Predict
df_predicted = model.transform(df_vectorized)

# Save predictions to PostgreSQL
df_predicted.write \
    .format("jdbc") \
    .option("url", os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/logsdb")) \
    .option("dbtable", "kmeans_predictions") \
    .option("user", os.getenv("POSTGRES_USER", "admin")) \
    .option("password", os.getenv("POSTGRES_PASSWORD", "admin")) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()