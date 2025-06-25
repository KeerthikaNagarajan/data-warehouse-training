from datetime import datetime, time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

def check_time(**context):
    exec_time = context['logical_date']  # Changed from execution_date
    if exec_time.weekday() >= 5:
        return 'weekend_skip'
    return 'morning_task' if exec_time.hour < 12 else 'afternoon_task'

with DAG(
    'time_based_tasks',
    start_date=datetime(2023, 1, 1),
    schedule='@hourly',  # Changed from schedule_interval
    catchup=False
) as dag:
    
    branch = PythonOperator(
        task_id='check_time',
        python_callable=check_time
    )
    
    morning = PythonOperator(
        task_id='morning_task',
        python_callable=lambda: print("Morning task running")
    )
    
    afternoon = PythonOperator(
        task_id='afternoon_task',
        python_callable=lambda: print("Afternoon task running")
    )
    
    weekend = EmptyOperator(task_id='weekend_skip')
    
    cleanup = EmptyOperator(
        task_id='cleanup',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )
    
    branch >> [morning, afternoon, weekend] >> cleanup