from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.pipeline import Pipeline
import os
import time
from datetime import datetime

# Create Spark session
spark = SparkSession.builder \
    .appName("NginxResponseTimePrediction") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.20") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

# Define schema (same as before)
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

# Function to train model (runs periodically)
def train_response_time_model():
    try:
        print(f"Training response time prediction model: {datetime.now()}")
        
        # Load historical data from PostgreSQL
        training_data = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
            .option("dbtable", """
                (SELECT 
                    endpoint,
                    request_method,
                    request_length::float as request_length,
                    EXTRACT(HOUR FROM timestamp) as hour_of_day,
                    status_code,
                    CASE WHEN geoip2_country_code = '' THEN 'UNKNOWN' ELSE geoip2_country_code END as country,
                    request_time_seconds
                FROM raw_logs
                WHERE 
                    timestamp > NOW() - INTERVAL '7 days'
                    AND request_time_seconds IS NOT NULL
                    AND request_time_seconds > 0
                    AND endpoint != ''
                LIMIT 100000) as training
            """) \
            .option("user", "superset") \
            .option("password", "superset") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        # If not enough data, exit
        if training_data.count() < 100:
            print("Not enough training data")
            return None
        
        # Feature engineering
        stages = []
        
        # Encode categorical features
        categorical_cols = ["endpoint", "request_method", "country"]
        for col_name in categorical_cols:
            # Convert string to index
            string_indexer = StringIndexer(
                inputCol=col_name, 
                outputCol=f"{col_name}_index",
                handleInvalid="keep"
            )
            stages.append(string_indexer)
            
            # Convert index to one-hot encoding
            encoder = OneHotEncoder(
                inputCols=[f"{col_name}_index"],
                outputCols=[f"{col_name}_vec"]
            )
            stages.append(encoder)
        
        # Numeric features
        numeric_cols = ["request_length", "hour_of_day", "status_code"]
        
        # Assemble features into a single vector
        assembler_inputs = [f"{c}_vec" for c in categorical_cols] + numeric_cols
        assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
        stages.append(assembler)
        
        # Random Forest regressor
        rf = RandomForestRegressor(
            featuresCol="features", 
            labelCol="request_time_seconds",
            numTrees=20,
            maxDepth=5
        )
        stages.append(rf)
        
        # Create pipeline
        pipeline = Pipeline(stages=stages)
        
        # Train model
        model = pipeline.fit(training_data)
        
        # Save model for inference
        model_path = "/tmp/models/response_time_model"
        model.write().overwrite().save(model_path)
        
        print(f"Model trained and saved to {model_path}")
        return model
        
    except Exception as e:
        print(f"Error training model: {str(e)}")
        return None

# Function to predict response times
def predict_response_times(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: No data")
        return
    
    try:
        # Extract features
        features_df = batch_df.select(
            "timestamp",
            "endpoint",
            "request_method",
            col("request_length").cast("float").alias("request_length"),
            hour(col("timestamp")).alias("hour_of_day"),
            "status_code",
            when(col("geoip2_country_code") == "", "UNKNOWN").otherwise(col("geoip2_country_code")).alias("country"),
            "request_time_seconds"
        ).filter(
            col("endpoint") != "" & 
            col("request_time_seconds").isNotNull() & 
            (col("request_time_seconds") > 0)
        )
        
        if features_df.count() == 0:
            print(f"Batch {batch_id}: No valid data for prediction")
            return
        
        # Load the latest model
        from pyspark.ml import PipelineModel
        model_path = "/tmp/models/response_time_model"
        
        try:
            model = PipelineModel.load(model_path)
        except:
            print(f"No model found at {model_path}, training new model")
            model = train_response_time_model()
            if model is None:
                print("Failed to train model")
                return
        
        # Make predictions
        predictions = model.transform(features_df)
        
        # Evaluate predictions
        predictions_df = predictions.select(
            "timestamp",
            "endpoint", 
            "request_method",
            col("prediction").alias("predicted_time"),
            col("request_time_seconds").alias("actual_time")
        )
        
        # Write predictions to PostgreSQL
        predictions_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/nginx_logs") \
            .option("dbtable", "response_time_predictions") \
            .option("user", "superset") \
            .option("password", "superset") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        # Calculate evaluation metrics
        evaluator = RegressionEvaluator(
            labelCol="actual_time",
            predictionCol="predicted_time",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        print(f"Batch {batch_id}: RMSE = {rmse}")
        
    except Exception as e:
        print(f"Error in batch {batch_id}: {str(e)}")

# Create streaming DataFrame from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BROKER", "kafka:9092")) \
    .option("subscribe", os.environ.get("KAFKA_NGINX_TOPIC", "nginx")) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON from Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .filter(col("http_user_agent") != "promtail/2.2.1") \
    .withColumn("timestamp", to_timestamp(col("time_iso8601"))) \
    .withColumn("request_time_seconds", col("request_time").cast(DoubleType())) \
    .withColumn("status_code", col("status").cast(IntegerType()))

# Extract API endpoint from request_uri
from pyspark.sql.functions import regexp_extract, hour
parsed_df = parsed_df.withColumn(
    "endpoint", 
    regexp_extract(col("request_uri"), "^/[^/]+/([^/]+)", 1)
)

# Train initial model
train_response_time_model()

# Process in micro-batches
query = parsed_df \
    .writeStream \
    .foreachBatch(predict_response_times) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/response_time_prediction") \
    .trigger(processingTime="1 minute") \
    .start()

# Periodically retrain the model
import threading
def periodic_training():
    while True:
        time.sleep(3600)  # Retrain every hour
        train_response_time_model()

# Start training thread
training_thread = threading.Thread(target=periodic_training)
training_thread.daemon = True
training_thread.start()

query.awaitTermination()