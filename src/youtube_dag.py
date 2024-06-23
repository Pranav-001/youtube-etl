from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from youtube_video_comments import run_youtube_etl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 21),
    "email": ["pramittal001@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "youtube_dag",
    default_args=default_args,
    description="Youtube DAG",
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id="complete_twitter_etl",
    python_callable=run_youtube_etl,
    dag=dag,
)

run_etl
