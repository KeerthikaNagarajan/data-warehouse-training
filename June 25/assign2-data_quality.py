from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
import pandas as pd

def validate_data():
    df = pd.read_csv('/opt/airflow/data/orders.csv')
    required = ['order_id', 'customer_id', 'amount']
    
    if not all(col in df.columns for col in required):
        raise AirflowFailException("Missing required columns")
    
    if df[required].isnull().any().any():
        raise AirflowFailException("Null values in mandatory fields")
    
    print("Validation passed")

def summarize():
    df = pd.read_csv('/opt/airflow/data/orders.csv')
    print(df.describe())

with DAG(
    'data_quality',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data
    )
    
    summarize = PythonOperator(
        task_id='summarize',
        python_callable=summarize
    )
    
    validate >> summarize