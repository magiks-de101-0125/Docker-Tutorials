from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import json


# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# Function to generate and push different types of data
def generate_data(**kwargs):
    """Creates a DataFrame, dictionary, and JSON, then pushes them to XCom."""

    df = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

    data_dict = {'name': 'Airflow', 'version': 2.6}

    data_json = json.dumps(data_dict)
    
    logging.info(f"Generated DataFrame:\n{df}")
    logging.info(f"Generated Dictionary: {data_dict}")
    logging.info(f"Generated JSON: {data_json}")
    
    # Push different data formats into XCom
    ti = kwargs['task_instance']

    ti.xcom_push(key='dataframe', value=df.to_dict())  # DataFrame as dictionary

    ti.xcom_push(key='dictionary', value=data_dict)  # Plain dictionary

    ti.xcom_push(key='json_data', value=data_json)  # JSON string


# Function to retrieve and log different types of data
def receive_data(**kwargs):
    """Retrieves and logs DataFrame, dictionary, and JSON from XCom."""
    ti = kwargs['task_instance']
    
    df_dict = ti.xcom_pull(task_ids='generate_data', key='dataframe')
    data_dict = ti.xcom_pull(task_ids='generate_data', key='dictionary')
    data_json = ti.xcom_pull(task_ids='generate_data', key='json_data')
    
    df = pd.DataFrame.from_dict(df_dict)
    
    logging.info(f"Received DataFrame:\n{df}")
    logging.info(f"Received Dictionary: {data_dict}")
    logging.info(f"Received JSON: {json.loads(data_json)}")


# Define the DAG
with DAG(
    dag_id='lesson2_xcom_data_types_dag',
    default_args=default_args,
    description='A DAG demonstrating XCom data passing with different data types',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson2', 'xcom', 'data_transfer']
) as dag:

    # Task 1: Generate and push data
    task_1 = PythonOperator(
        task_id='generate_data',
        python_callable=generate_data,
    )

    # Task 2: Retrieve and log data
    task_2 = PythonOperator(
        task_id='receive_data',
        python_callable=receive_data,
    )

    # Define task dependencies
    task_1 >> task_2
