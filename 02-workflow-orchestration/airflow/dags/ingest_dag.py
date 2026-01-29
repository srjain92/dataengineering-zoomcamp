import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# 1. Default arguments: What happens if a task fails?
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Define the DAG
dag = DAG(
    'taxi_ingest_v02',
    default_args=default_args,
    description='Testing our Docker ingestion worker',
    schedule_interval='@monthly',       # Only run when we click "Play"
    start_date=datetime(2020, 1, 1),    # A fixed start date
    end_date=datetime(2020, 3, 1),      # A fixed end date
    catchup=True,                       # DO NOT run historical dates yet if it is set to False
    max_active_runs=2,                  # Only two active run at a time                   
)

# 3. The Task
ingest_task = DockerOperator(
    task_id='ingest_taxi_data',
    image='taxi_ingest:v001',
    # Make the container name unique per run!
    container_name='ingest_worker_{{ ds_nodash }}',
    api_version='auto',
    auto_remove=True,
    network_mode='airflow_default',
    # Pass the environment variables your script expects
    environment={
        'DB_USER': 'airflow',
        'DB_PASSWORD': 'airflow',
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'DB_NAME': 'airflow',
    },
    # The "ds" variable is Airflow's default for "YYYY-MM-DD"
    # The "ds_nodash" variable is "YYYYMMDD"
    command=[
    "--table_name", "green_taxi_{{ ds_nodash[:6] }}",
    "--url", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m') }}.csv.gz"
    ],
    # The actual command flags
    #command=[
    #   "--table_name", "green_taxi_2021_01",
    #    "--url", "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2021-01.csv.gz"
    #],
    dag=dag,
)