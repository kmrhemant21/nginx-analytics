from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, when
import mlflow
import mlflow.spark
import os
from datetime import datetime

# Set up MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minioadmin"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minioadmin"

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Train_Model") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

def train_model():
    # Start MLflow run
    with mlflow.start_run(run_name="train_nginx_model") as run:
        # Load training data
        training_data = spark.read.parquet("/tmp/nginx_training_data")
        
        # Feature preparation
        # Define slow response threshold (e.g., 95th percentile)
        response_time_threshold = training_data \
            .approxQuantile("request_time", [0.95], 0.05)[0]
        
        # Create target variable (1 for slow responses, 0 for normal)
        training_data = training_data.withColumn(
            "is_slow",
            when(col("request_time") > response_time_threshold, 1).otherwise(0)
        )
        
        # Log parameters
        mlflow.log_param("response_time_threshold", response_time_threshold)
        mlflow.log_param("training_records", training_data.count())
        
        # Split data into training and test sets
        train_df, test_df = training_data.randomSplit([0.8, 0.2], seed=42)
        
        # Define categorical and numeric features
        categorical_cols = ["endpoint", "request_method", "geoip2_country_code", "status_code"]
        numeric_cols = ["request_length", "body_bytes_sent", "bytes_sent", 
                       "hour_of_day", "day_of_week"]
        
        # Build ML Pipeline
        stages = []
        
        # Process categorical features
        for categorical_col in categorical_cols:
            # Handle nulls
            training_data = training_data.fillna("UNKNOWN", subset=[categorical_col])
            
            # String Indexing
            string_indexer = StringIndexer(
                inputCol=categorical_col,
                outputCol=f"{categorical_col}_index",
                handleInvalid="keep"
            )
            stages.append(string_indexer)
            
            # One-Hot Encoding
            encoder = OneHotEncoder(
                inputCols=[f"{categorical_col}_index"],
                outputCols=[f"{categorical_col}_encoded"]
            )
            stages.append(encoder)
        
        # Assemble features
        assembler_inputs = [f"{col}_encoded" for col in categorical_cols] + numeric_cols
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features",
            handleInvalid="skip"
        )
        stages.append(assembler)
        
        # Define classifier
        rf = RandomForestClassifier(
            labelCol="is_slow",
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        stages.append(rf)
        
        # Create and fit pipeline
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(train_df)
        
        # Evaluate model
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="is_slow", 
            predictionCol="prediction", 
            metricName="accuracy"
        )
        
        # Calculate metrics
        accuracy = evaluator.evaluate(predictions)
        evaluator.setMetricName("weightedPrecision")
        precision = evaluator.evaluate(predictions)
        evaluator.setMetricName("weightedRecall")
        recall = evaluator.evaluate(predictions)
        evaluator.setMetricName("f1")
        f1 = evaluator.evaluate(predictions)
        
        # Log metrics
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1", f1)
        
        # Log model
        mlflow.spark.log_model(
            model, 
            "model",
            registered_model_name="nginx_anomaly_detector"
        )
        
        # Log feature importance
        feature_importance = model.stages[-1].featureImportances.toArray()
        for i, importance in enumerate(feature_importance):
            mlflow.log_metric(f"feature_importance_{i}", importance)
        
        # Save metrics for next steps
        model_version = datetime.now().strftime("%Y%m%d%H%M%S")
        metrics = {
            "run_id": run.info.run_id,
            "model_version": model_version,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "records_count": training_data.count(),
            "response_time_threshold": response_time_threshold
        }
        
        # Save model metadata to ClickHouse
        metrics_df = spark.createDataFrame([metrics])
        metrics_df.write \
            .format("jdbc") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("url", "jdbc:clickhouse://clickhouse:8123/nginx_logs") \
            .option("dbtable", "model_metrics") \
            .option("user", "default") \
            .option("password", "") \
            .mode("append") \
            .save()
        
        return metrics

if __name__ == "__main__":
    result = train_model()
    print(f"Training complete: {result}")
    spark.stop()