from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from etl.ingest import load_table
from etl.transform import clean_df
from etl.load import save_to_parquet

def etl_task():
    df = load_table("Roster")
    df = clean_df(df)
    save_to_parquet(df, "processed/roster.parquet")

with DAG(
    dag_id="etl_dag",
    description="ETL pipeline for Raptors data",
    start_date=datetime(2025, 9, 10),
    catchup=False,
    #None for manual running
    schedule=None
) as dag:
    run_etl = PythonOperator(
        task_id="run_etl_task",
        python_callable=etl_task
    )