from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import csv

def check_file_exists():
    if not os.path.exists('/opt/airflow/dags/customers.csv'):
        raise FileNotFoundError('d/opt/airflow/dags/customers.csv does not exist')

def count_rows():
    with open('/opt/airflow/dags/customers.csv', 'r') as f:
        reader = csv.reader(f)
        row_count = sum(1 for row in reader)
    return row_count

def log_row_count(**context):
    count = context['ti'].xcom_pull(task_ids='count_rows')
    print(f"Total rows: {count}")
    return count

with DAG('csv_summary', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    check_file = PythonOperator(
        task_id='check_file_exists',
        python_callable=check_file_exists
    )
    
    count_rows = PythonOperator(
        task_id='count_rows',
        python_callable=count_rows
    )
    
    log_count = PythonOperator(
        task_id='log_row_count',
        python_callable=log_row_count,
        provide_context=True
    )
    
    alert_if_large = BashOperator(
        task_id='alert_if_large',
        bash_command='echo "Row count exceeds 100!"',
        trigger_rule='one_success'
    )
    
    check_file >> count_rows >> log_count
    log_count >> alert_if_large