from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sync_dag = DAG(
    'git_sync_daily',
    default_args=default_args,
    description='Daily sync of DAGs and scripts from Git',
    schedule_interval='0 5 * * *',  # Runs at 5:00 AM every day
    start_date=datetime(2025, 7, 21),
    catchup=False
)

sync_command = """
cd /opt/airflow/dags
git config --global --add safe.directory /opt/airflow/dags
git reset --hard
git clean -fd
git fetch origin main
git checkout origin/main -- dags/
mv dags/* .
rmdir dags/
git checkout origin/main -- py_scripts/
"""

sync_task = BashOperator(
    task_id='git_sync',
    bash_command=sync_command,
    dag=sync_dag
)