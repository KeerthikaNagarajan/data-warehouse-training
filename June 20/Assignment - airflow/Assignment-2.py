from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import os

def process_sales():
    start_time = datetime.now()
    
    df = pd.read_csv('/opt/airflow/dags/sales.csv')
    summary = df.groupby('category')['amount'].sum().reset_index()
    summary.to_csv('/opt/airflow/dags/sales_summary.csv', index=False)
    
    os.rename('/opt/airflow/dags/sales.csv', 'archive/sales.csv')
    
    processing_time = (datetime.now() - start_time).total_seconds()
    if processing_time > 300:
        raise ValueError("Processing took more than 5 minutes")

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG('daily_sales', 
         start_date=datetime(2023, 1, 1), 
         schedule_interval='0 6 * * *',
         default_args=default_args) as dag:
    
    process_sales = PythonOperator(
        task_id='process_sales',
        python_callable=process_sales,
        execution_timeout=timedelta(minutes=5)
    )