from pyspark.sql import SparkSession
import mlflow
from mlflow.tracking import MlflowClient
import os

# Set up MLflow
mlflow.set_tracking_uri("http://mlflow:5000")
os.environ['MLFLOW_S3_ENDPOINT_URL'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minioadmin"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minioadmin"

# Create Spark session
spark = SparkSession.builder \
    .appName("NGINX_Register_Model") \
    .getOrCreate()

def register_model():
    # Get the latest run
    client = MlflowClient()
    
    # Get the most recent run for the nginx model training
    runs = mlflow.search_runs(
        experiment_names=["Default"],
        filter_string="tags.mlflow.runName='train_nginx_model'",
        order_by=["attributes.start_time DESC"],
        max_results=1
    )
    
    if len(runs) == 0:
        print("No training runs found")
        return None
    
    latest_run_id = runs.iloc[0]["run_id"]
    print(f"Latest run ID: {latest_run_id}")
    
    # Get model details
    model_uri = f"runs:/{latest_run_id}/model"
    
    # Register model
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name="nginx_anomaly_detector"
    )
    
    print(f"Model registered: {registered_model.name} v{registered_model.version}")
    
    # Get the latest metrics
    metrics = {
        "run_id": latest_run_id,
        "model_version": registered_model.version,
        "model_uri": model_uri
    }
    
    # Get accuracy from the run
    run = mlflow.get_run(latest_run_id)
    accuracy = run.data.metrics.get("accuracy", 0)
    
    # If accuracy is good enough, transition to Production
    if accuracy >= 0.85:
        client.transition_model_version_stage(
            name="nginx_anomaly_detector",
            version=registered_model.version,
            stage="Production"
        )
        print(f"Model v{registered_model.version} promoted to Production")
        metrics["stage"] = "Production"
    else:
        client.transition_model_version_stage(
            name="nginx_anomaly_detector",
            version=registered_model.version,
            stage="Staging"
        )
        print(f"Model v{registered_model.version} promoted to Staging (accuracy below threshold)")
        metrics["stage"] = "Staging"
    
    return metrics

if __name__ == "__main__":
    result = register_model()
    print(f"Registration complete: {result}")
    spark.stop()