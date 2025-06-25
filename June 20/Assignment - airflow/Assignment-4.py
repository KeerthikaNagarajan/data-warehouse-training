from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import os

def check_file_size():
    size = os.path.getsize('/opt/airflow/dags/inventory.csv')
    return 'light_summary' if size < 500000 else 'detailed_processing'

def light_summary():
    print("Running light summary for small file")

def detailed_processing():
    print("Running detailed processing for large file")

def cleanup():
    print("Running common cleanup")

with DAG('branching', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    check_size = BranchPythonOperator(
        task_id='check_size',
        python_callable=check_file_size
    )
    
    light_task = PythonOperator(
        task_id='light_summary',
        python_callable=light_summary
    )
    
    detail_task = PythonOperator(
        task_id='detailed_processing',
        python_callable=detailed_processing
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup',
        python_callable=cleanup,
        trigger_rule='none_failed_or_skipped'
    )
    
    check_size >> [light_task, detail_task] >> cleanup_task