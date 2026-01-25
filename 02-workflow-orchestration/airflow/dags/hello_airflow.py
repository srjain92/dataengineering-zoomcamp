from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="00_hello_airflow_v01",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # This means it only runs when you click 'Play'
    catchup=False
) as dag:

    task_1 = BashOperator(
        task_id="greet",
        bash_command="echo 'Success! My Airflow setup is working.'"
    )