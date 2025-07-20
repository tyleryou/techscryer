from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import make_aware
from pytz import timezone

# Import from the new location (assuming you moved files)
from py_scripts.bronze__ingest_igdb import main

# Timezone setup
central_tz = timezone('US/Central')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': make_aware(datetime(2025, 7, 20), central_tz),
}

with DAG(
    'bronze__igdb',
    default_args=default_args,
    description='Bronze layer of raw IGDB data ingestion',
    schedule_interval=timedelta(days=1),  # Runs daily
    catchup=False,
    tags=['igdb', 'bronze'],
    max_active_runs=1,
) as dag:

    # This is how you "call" the task in Airflow
    # Just defining it here makes it part of the DAG
    ingest_task = PythonOperator(
        task_id='ingest_igdb_data',
        python_callable=main,
        execution_timeout=timedelta(minutes=30),
    )

    # No explicit call needed - Airflow will execute it
    # when the DAG runs