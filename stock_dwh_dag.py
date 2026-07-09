"""
stock_dwh_dag.py

Airflow 3.x DAG for the Stock Market Analytics Warehouse.
Orchestrates the existing extract.py -> transform.py -> load.py scripts,
UNCHANGED, via BashOperator -- same idea as pipeline.py's
subprocess.run([sys.executable, script]) pattern, now with per-task
retries, a run history, and a UI.

Place this file in ~/airflow/dags/stock_dwh_dag.py
"""


from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = f"/home/ubuntu/stock_dwh"
PYTHON_BIN = f"{PROJECT_DIR}/venv/bin/python3"

default_args = {
    'owner': "data-eng",
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="stock_dwh_pipeline",
    description="Daily ETL: Alpha Vantage API -> S3 -> RDS Star Schema",
    default_args=default_args,
    schedule="0 18 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock-dwh", "etl"],

) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} extract.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && {PYTHON_BIN} load.py"
    )

    extract >> transform >> load