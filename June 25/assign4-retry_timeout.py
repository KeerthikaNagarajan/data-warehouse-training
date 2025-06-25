from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import random

def unreliable_task():
    if random.random() < 0.7:  # 70% chance to fail
        time.sleep(25)
        raise Exception("Random failure")
    print("Success!")

with DAG(
    'retry_timeout_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(seconds=10),
        'retry_exponential_backoff': True
    },
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='unreliable_task',
        python_callable=unreliable_task,
        execution_timeout=timedelta(seconds=30)
    )