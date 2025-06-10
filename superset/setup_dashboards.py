import json
import requests
import time
import os

SUPERSET_URL = "http://superset:8088"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin"

def get_access_token():
    login_data = {
        "username": ADMIN_USERNAME,
        "password": ADMIN_PASSWORD,
        "provider": "db"
    }
    
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json=login_data
    )
    
    if resp.status_code != 200:
        raise Exception(f"Failed to login: {resp.text}")
    
    return resp.json()["access_token"]

def create_database_connection(token):
    # Create PostgreSQL connection
    database_data = {
        "database_name": "Nginx Logs",
        "sqlalchemy_uri": "postgresql+psycopg2://superset:superset@postgres:5432/nginx_logs"
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/database/",
        headers=headers,
        json=database_data
    )
    
    if resp.status_code == 200:
        print("Database connection created successfully")
        return resp.json()["id"]
    else:
        print(f"Failed to create database connection: {resp.text}")
        return None

def create_dataset(token, database_id, table_name, dataset_name):
    # Create dataset
    dataset_data = {
        "database": database_id,
        "schema": "public",
        "table_name": table_name,
        "sql": f"SELECT * FROM {table_name}"
    }
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    resp = requests.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        headers=headers,
        json=dataset_data
    )
    
    if resp.status_code == 201:
        print(f"Dataset {dataset_name} created successfully")
        return resp.json()["id"]
    else:
        print(f"Failed to create dataset: {resp.text}")
        return None

def main():
    # Wait for Superset to be ready
    time.sleep(60)
    
    # Get access token
    token = get_access_token()
    
    # Create database connection
    database_id = create_database_connection(token)
    
    if database_id:
        # Create datasets
        create_dataset(token, database_id, "raw_logs", "NGINX Logs")
        create_dataset(token, database_id, "anomalies", "NGINX Anomalies")
        create_dataset(token, database_id, "response_time_predictions", "Response Time Predictions")
        
        print("Superset setup completed successfully")
    else:
        print("Superset setup failed")

if __name__ == "__main__":
    main()