import os
import gzip
import shutil
import requests
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# --- 1. GOOGLE CLOUD & URL INFO ---
PROJECT_ID = "nyc-taxi-data-pipeline-485822"
BUCKET = "nyc-taxi-data-pipeline-485822-bucket"

# --- 2. DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='yellow_taxi_data_ingestion_parquet_v1',
    schedule_interval="@monthly",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 6, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=3 # Limit parallelism
) as dag:

    # --- 3. TASK DEFINITIONS ---    
    @task
    def download_data(ds=None):
        # Extract the month from the execution date (ds) provided by Airflow
        # 'ds' looks like '2020-01-01'
        year_month = ds[:7]  # '2020-01'

        file_name = f"yellow_tripdata_{year_month}"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}.parquet"

        local_path = f"/tmp/{file_name}.parquet"
        print(f"Downloading from {url}...")
        response = requests.get(url)

        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        return local_path

    # The GCS Operator can use Jinja directly in its strings
    # {{ ds[:7] }} will automatically resolve to '2020-01', '2020-02', etc.
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/yellow_tripdata_{{ ds[:7] }}.parquet",        
        dst="raw/yellow/yellow_tripdata_{{ ds[:7] }}.parquet",  # The path inside your bucket
        bucket=BUCKET,                  # Your bucket variable
        gcp_conn_id="google_cloud_default" # Our verified connection
    )

    @task
    def cleanup_data(local_path):
        # Check if files exist before trying to delete to avoid errors
        if os.path.exists(local_path):
            os.remove(local_path)
            print(f"Deleted temporary file: {local_path}")

    # --- 4. THE ORDER (Wiring the tasks together) ---
    
    # 1. Start by calling the download task to capture the path
    file_path = download_data()
    
    # 2. Set the dependencies for the traditional operator and cleanup
    # We tell Airflow: Download -> Upload -> Cleanup
    file_path >> upload_to_gcs >> cleanup_data(file_path)