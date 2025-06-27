from pyspark.sql import SparkSession
import mlflow
from mlflow.tracking import MlflowClient
import os
import json

# Set up MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minioadmin"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minioadmin"

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Evaluate_Model") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.3.2-patch9") \
    .getOrCreate()

def evaluate_model():
    # Get the latest production model
    client = MlflowClient()
    
    # Find the production model
    production_models = client.get_latest_versions(
        name="nginx_anomaly_detector",
        stages=["Production"]
    )
    
    if not production_models:
        print("No production model found")
        staging_models = client.get_latest_versions(
            name="nginx_anomaly_detector",
            stages=["Staging"]
        )
        if not staging_models:
            print("No staging model found either")
            return None
        model_version = staging_models[0]
        print(f"Using staging model version: {model_version.version}")
    else:
        model_version = production_models[0]
        print(f"Using production model version: {model_version.version}")
    
    # Get run ID from model version
    run_id = model_version.run_id
    
    # Get metrics from run
    run = mlflow.get_run(run_id)
    metrics = run.data.metrics
    params = run.data.params
    
    # Prepare metrics for ClickHouse
    metrics_json = json.dumps(metrics)
    params_json = json.dumps(params)
    
    # Get model version info
    model_info = {
        "run_id": run_id,
        "model_version": model_version.version,
        "metrics_json": metrics_json,
        "parameters_json": params_json,
        "accuracy": metrics.get("accuracy", 0),
        "precision": metrics.get("precision", 0),
        "recall": metrics.get("recall", 0),
        "f1_score": metrics.get("f1", 0),
        "records_count": int(params.get("training_records", 0))
    }
    
    # Create DataFrame for ClickHouse
    metrics_df = spark.createDataFrame([model_info])
    
    # Write to ClickHouse
    metrics_df.write \
        .format("jdbc") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/nginx_logs") \
        .option("dbtable", "model_metrics") \
        .option("user", "default") \
        .option("password", "") \
        .mode("append") \
        .save()
    
    return model_info

if __name__ == "__main__":
    result = evaluate_model()
    print(f"Evaluation complete: {result}")
    spark.stop()