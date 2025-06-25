from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    'parent_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    trigger_child = TriggerDagRunOperator(
        task_id='trigger_child',
        trigger_dag_id='child_dag',
        conf={'parent_execution_date': '{{ ds }}'}  
    )