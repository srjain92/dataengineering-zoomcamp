from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from datetime import datetime

with DAG(
    dag_id='test_gcp_connection',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False
) as dag:

    # Replace with your actual bucket name from Terraform
    list_files = GCSListObjectsOperator(
        task_id='list_files_in_bucket',
        bucket='nyc-taxi-data-pipeline-485822-bucket', 
        gcp_conn_id='google_cloud_default'
    )