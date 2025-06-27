from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import requests
import json
import os
from datetime import datetime, timedelta

# Default arguments for DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Path to scripts and config
SPARK_SCRIPTS_PATH = '/opt/airflow/dags/scripts'

# Send email with model results
def send_model_notification(**context):
    """Send notification email with model metrics"""
    model_metrics = context['ti'].xcom_pull(task_ids='train_model')
    
    if not model_metrics:
        print("No model metrics available")
        return
    
    smtp_server = os.environ.get("SMTP_SERVER", "smtp.example.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "alerts@example.com")
    smtp_password = os.environ.get("SMTP_PASSWORD", "password")
    email_recipients = os.environ.get("EMAIL_RECIPIENTS", "ops@example.com")
    
    subject = f"NGINX Log Analysis: Model Training Results"
    
    # Create email content with actionable information
    body = f"""
    <html>
    <body>
        <h2>NGINX Log Analysis Model Training Results</h2>
        <p>A new model has been trained and registered in MLflow.</p>
        
        <h3>Model Performance:</h3>
        <ul>
            <li>Accuracy: {model_metrics.get('accuracy', 'N/A')}</li>
            <li>Precision: {model_metrics.get('precision', 'N/A')}</li>
            <li>Recall: {model_metrics.get('recall', 'N/A')}</li>
            <li>F1 Score: {model_metrics.get('f1_score', 'N/A')}</li>
            <li>Training Records: {model_metrics.get('records_count', 'N/A')}</li>
            <li>Model Version: {model_metrics.get('model_version', 'latest')}</li>
        </ul>
        
        <h3>Required Actions:</h3>
        <ol>
            <li><strong>Review Model Performance</strong>: Check if the metrics above meet expected thresholds.</li>
            <li><strong>Verify Model in Production</strong>: The model has been automatically registered. Visit the MLflow UI to promote it to production if metrics are satisfactory.</li>
            <li><strong>Check Anomaly Detection Dashboard</strong>: Verify the new model is correctly identifying anomalies.</li>
        </ol>
        
        <h3>Resources:</h3>
        <ul>
            <li>MLflow UI: <a href="http://localhost:5000">http://localhost:5000</a></li>
            <li>ClickHouse Dashboard: <a href="http://localhost:8123/play">http://localhost:8123/play</a></li>
            <li>Anomaly Detection API: <a href="http://localhost:5001/invocations">http://localhost:5001/invocations</a></li>
        </ul>
        
        <p>This is an automated message from the NGINX ML Pipeline.</p>
    </body>
    </html>
    """
    
    # Email sending logic here
    print(f"Would send email with subject: {subject}")
    print(f"Email content: {body}")

# Daily model training DAG
with DAG(
    'nginx_model_training',
    default_args=default_args,
    description='Train NGINX anomaly detection model daily',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    start_date=days_ago(1),
    catchup=False,
    tags=['nginx', 'mlops'],
) as dag:

    # 1. Extract training data from ClickHouse
    extract_training_data = SparkSubmitOperator(
        task_id='extract_training_data',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/extract_training_data.py",
        name='extract_nginx_training_data',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.maxResultSize': '1g'
        },
        jars='/opt/bitnami/spark/jars/clickhouse-jdbc-0.3.2-patch9-all.jar,/opt/bitnami/spark/jars/postgresql-42.5.0.jar',
        verbose=True
    )
    
    # 2. Train model using Spark ML
    train_model = SparkSubmitOperator(
        task_id='train_model',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/train_model.py",
        name='train_nginx_anomaly_model',
        conf={
            'spark.driver.memory': '4g',
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.driver.maxResultSize': '2g'
        },
        verbose=True
    )
    
    # 3. Register model in MLflow
    register_model = SparkSubmitOperator(
        task_id='register_model',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/register_model.py",
        name='register_nginx_model',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g'
        },
        verbose=True
    )
    
    # 4. Evaluate model performance
    evaluate_model = SparkSubmitOperator(
        task_id='evaluate_model',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/evaluate_model.py",
        name='evaluate_nginx_model',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g'
        },
        verbose=True
    )
    
    # 5. Send notification
    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=send_model_notification,
        provide_context=True
    )
    
    # Define task dependencies
    extract_training_data >> train_model >> register_model >> evaluate_model >> send_notification

# Hourly anomaly detection DAG
with DAG(
    'nginx_anomaly_detection',
    default_args=default_args,
    description='Run NGINX anomaly detection hourly',
    schedule_interval='0 * * * *',  # Run every hour
    start_date=days_ago(1),
    catchup=False,
    tags=['nginx', 'mlops'],
) as anomaly_dag:
    
    # 1. Run anomaly detection on recent logs
    detect_anomalies = SparkSubmitOperator(
        task_id='detect_anomalies',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/detect_anomalies.py",
        name='detect_nginx_anomalies',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g'
        },
        verbose=True
    )
    
    # 2. Store anomalies in ClickHouse
    store_anomalies = SparkSubmitOperator(
        task_id='store_anomalies',
        conn_id='spark_default',
        application=f"{SPARK_SCRIPTS_PATH}/store_anomalies.py",
        name='store_nginx_anomalies',
        conf={
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g'
        },
        jars='/opt/bitnami/spark/jars/clickhouse-jdbc-0.3.2-patch9-all.jar',
        verbose=True
    )
    
    # Define task dependencies
    detect_anomalies >> store_anomalies