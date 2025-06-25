from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import shutil
import os

def process_file():
    with open('/opt/airflow/data/incoming/report.csv', 'r') as f:
        print(f.read())
    os.makedirs('/opt/airflow/data/archive', exist_ok=True)
    shutil.move('/opt/airflow/data/incoming/report.csv', 
               '/opt/airflow/data/archive/report.csv')

with DAG(
    'file_sensor_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/opt/airflow/data/incoming/report.csv',
        poke_interval=30,
        timeout=60*10,
        mode='poke'
    )
    
    process = PythonOperator(
        task_id='process_file',
        python_callable=process_file
    )
    
    wait_for_file >> process