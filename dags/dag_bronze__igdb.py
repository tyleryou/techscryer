from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.timezone import make_aware
from pytz import timezone
from py_scripts.bronze__ingest_igdb import main

# Timezone setup
central_tz = timezone('US/Central')

ENDPOINTS = [
    'games',
    'popularity_types',
    'popularity_primitives',
    'companies',
    'game_engines',
    'game_modes',
    'game_statuses',
    'game_time_to_beats',
    'game_types',
    'genres',
    'keywords',
    'platforms'
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': make_aware(datetime(2025, 7, 21), central_tz),
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
    
    tasks = {}
    for endpoint in ENDPOINTS:
        task_id = f'ingest_{endpoint}'
        tasks[endpoint] = PythonOperator(
            task_id=task_id,
            python_callable=main,
            op_kwargs={'endpoint': endpoint},
            execution_timeout=timedelta(minutes=30),
        )
