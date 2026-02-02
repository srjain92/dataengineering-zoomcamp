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
    dag_id='green_taxi_data_ingestion_final_v1',
    schedule_interval="@monthly",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2020, 12, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    max_active_tasks=3 # Limit parallelism
) as dag:

    # --- 3. TASK DEFINITIONS ---    
    @task
    def download_data(**kwargs):
        # Extract the month from the execution date (ds) provided by Airflow
        # 'ds' looks like '2020-01-01'
        ds = kwargs['ds']
        year_month = ds[:7]  # '2020-01'

        file_name = f"green_tripdata_{year_month}"
        url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{file_name}.csv.gz" 

        zip_path = f"/tmp/{file_name}.csv.gz"
        print(f"Downloading from {url}...")
        response = requests.get(url)

        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        return {"zip_path": zip_path, "year_month": year_month}

    @task
    def unzip_data(file_info):
        zip_path = file_info['zip_path']
        # We define the output path by removing the '.gz' extension
        csv_path = zip_path.replace('.gz', '')
        
        print(f"Unzipping {zip_path}...")
        with gzip.open(zip_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        return csv_path


    # The GCS Operator can use Jinja directly in its strings
    # {{ ds[:7] }} will automatically resolve to '2020-01', '2020-02', etc.
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/green_tripdata_{{ ds[:7] }}.csv",        # The file we unzipped
        dst="raw/green/green_tripdata_{{ ds[:7] }}.csv",  # The path inside your bucket
        bucket=BUCKET,                  # Your bucket variable
        gcp_conn_id="google_cloud_default" # Our verified connection
    )

    @task
    def cleanup_data(file_info, csv_path):
        zip_path = file_info['zip_path']
        # Check if files exist before trying to delete to avoid errors
        for path in [zip_path, csv_path]:
            if os.path.exists(path):
                os.remove(path)
                print(f"Deleted temporary file: {path}")


    # --- 4. THE ORDER (Wiring the tasks together) ---
    
    # 1. Start by calling the download task
    info = download_data()
    
    # 2. Pass the result of download into the unzip task
    csv_file_path = unzip_data(info)
    
    # 3. Set the dependencies for the traditional operator and cleanup
    # We tell Airflow: Unzip -> Upload -> Cleanup
    csv_file_path >> upload_to_gcs >> cleanup_data(info, csv_file_path)