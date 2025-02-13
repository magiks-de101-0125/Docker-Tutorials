from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import random

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# Function to generate a random number and push it to XCom
def generate_number(**kwargs):
    random_number = random.randint(1, 100)
    logging.info(f"Generated number: {random_number}")
    kwargs['task_instance'].xcom_push(key='generated_value', value=random_number)  # Key changed


# Function to retrieve the number, transform it, and push back to XCom
def double_number(**kwargs):
    ti = kwargs['task_instance']
    number = ti.xcom_pull(task_ids='generate_number', key='generated_value')  # Clearly references generate_number task
    doubled_number = number * 2
    logging.info(f"Doubled number: {doubled_number}")
    ti.xcom_push(key='transformed_value', value=doubled_number)  # Key changed


# Function to retrieve the transformed number and log the result
def log_final_value(**kwargs):
    ti = kwargs['task_instance']
    final_value = ti.xcom_pull(task_ids='double_number', key='transformed_value')  # Clearly references double_number task
    logging.info(f"Final Value after transformation: {final_value}")


# Define the DAG
with DAG(
    dag_id='lesson2_xcom_dag',
    default_args=default_args,
    description='A DAG demonstrating XCom variable passing between tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson2', 'xcom']
) as dag:

    # Task 1: Generate a random number
    task_1 = PythonOperator(
        task_id='generate_number',
        python_callable=generate_number,
        provide_context=True
    )

    # Task 2: Double the number
    task_2 = PythonOperator(
        task_id='double_number',
        python_callable=double_number,
        provide_context=True
    )

    # Task 3: Log the final value
    task_3 = PythonOperator(
        task_id='log_final_value',
        python_callable=log_final_value,
        provide_context=True
    )

    # Define task dependencies
    task_1 >> task_2 >> task_3
