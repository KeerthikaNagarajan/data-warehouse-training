from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import random

def flaky_api_call():
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("API call failed")
    return "API call succeeded"

def on_failure_callback(context):
    print("All retries failed! Sending alert...")
    # Add actual alert mechanism here (email, Slack, etc.)

def success_task(**context):
    print("API succeeded, running success task")
    print(context['ti'].xcom_pull(task_ids='api_call'))

default_args = {
    'retries': 3,
    'retry_delay': timedelta(seconds=10),  # Exponential backoff would need custom code
    'on_failure_callback': on_failure_callback
}

with DAG('flaky_api', 
         start_date=datetime(2023, 1, 1), 
         schedule_interval=None,
         default_args=default_args) as dag:
    
    api_call = PythonOperator(
        task_id='api_call',
        python_callable=flaky_api_call
    )
    
    success_handler = PythonOperator(
        task_id='success_handler',
        python_callable=success_task,
        provide_context=True,
        trigger_rule='all_success'
    )
    
    api_call >> success_handler