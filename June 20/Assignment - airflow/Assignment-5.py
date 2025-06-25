from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import csv
import pandas as pd

@task
def get_csv_files():
    files = [f for f in os.listdir('/opt/airflow/dags/input') if f.endswith('.csv')]
    return files

@task
def process_file(file_name):
    with open(f'/opt/airflow/dags/input/{file_name}', 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)
        row_count = sum(1 for row in reader)
    
    summary = {
        'file_name': file_name,
        'headers': ','.join(headers),
        'row_count': row_count
    }
    
    pd.DataFrame([summary]).to_csv(f'/opt/airflow/dags/summaries/{file_name}', index=False)
    return summary

@task
def merge_summaries(summaries):
    df = pd.DataFrame(summaries)
    df.to_csv('/opt/airflow/dags/merged_summary.csv', index=False)
    return '/opt/airflow/dags/merged_summary.csv'

with DAG('dynamic_files', start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    files = get_csv_files()
    processed_files = process_file.expand(file_name=files)
    merge_results = merge_summaries(processed_files)