from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import random

def task_may_fail():
    if random.random() < 0.3:  # 30% chance to fail
        raise Exception("Intentional failure for testing")
    print("Task succeeded")

def send_success_email(context):
    send_email(
        to=["nkeerthika2004@gmail.com"],
        subject=f"✅ Success: {context['dag'].dag_id}",
        html_content=f"""
        <h3>DAG Run Completed</h3>
        <p><b>DAG:</b> {context['dag'].dag_id}</p>
        <p><b>Execution Time:</b> {context['ts']}</p>
        """
    )

default_args = {
    'on_failure_callback': lambda ctx: send_email(
        to=["nkeerthika2004@gmail.com"],
        subject=f"❌ Failed: {ctx['task_instance'].task_id}",
        html_content=f"""
        <h3>Task Failed</h3>
        <p><b>Task:</b> {ctx['task_instance'].task_id}</p>
        <p><b>Log:</b> <a href="{ctx.get('task_instance').log_url}">View Logs</a></p>
        """
    )
}

with DAG(
    'email_notifications',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    default_args=default_args,
    catchup=False
) as dag:
    
    test_task = PythonOperator(
        task_id='test_task',
        python_callable=task_may_fail
    )
    
    success_email = PythonOperator(
        task_id='success_email',
        python_callable=send_success_email,
        trigger_rule='all_success'
    )
    
    test_task >> success_email