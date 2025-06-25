from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def log_conf(**kwargs):
    print(f"Received from parent: {kwargs['dag_run'].conf}")

with DAG(
    'child_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    log_conf = PythonOperator(
        task_id='log_conf',
        python_callable=log_conf,
        op_kwargs={'dag_run': '{{ dag_run }}'} 
        
    )