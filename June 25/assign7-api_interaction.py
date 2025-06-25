from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json

def process_response(response):
    try:
        data = json.loads(response)
        print(f"API Response: {data}")
        
        # Save to JSON file
        with open('/opt/airflow/data/api_response.json', 'w') as f:
            json.dump(data, f, indent=2)
            
    except Exception as e:
        raise ValueError(f"Failed to process response: {str(e)}")

with DAG(
    'api_interaction_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['assignment'],
) as dag:
    
    # Task 1: Call public API
    get_weather = SimpleHttpOperator(
        task_id='get_weather_data',
        method='GET',
        endpoint='/data/2.5/weather?q=London&appid=YOUR_API_KEY',  # Free API from openweathermap.org
        http_conn_id='http_default',
        response_filter=lambda response: response.text,
    )
    
    # Task 2: Process response
    process_data = PythonOperator(
        task_id='process_api_response',
        python_callable=process_response,
        op_args=["{{ ti.xcom_pull(task_ids='get_weather_data') }}"],
    )
    
    get_weather >> process_data