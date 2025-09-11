from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # or "0 0 * * *" for daily
    catchup=False
) as dag:
    task1 = EmptyOperator(task_id="dummy_task")