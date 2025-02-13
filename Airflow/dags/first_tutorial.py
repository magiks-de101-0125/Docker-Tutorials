from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the functions to be executed by PythonOperator
def task_a():
    logging.info("Task A: Starting process...")
    time.sleep(2)
    logging.info("Task A: Completed.")

def task_b():
    logging.info("Task B: Processing data...")
    time.sleep(3)
    logging.info("Task B: Completed.")

def task_c():
    logging.info("Task C: Performing analysis...")
    time.sleep(4)
    logging.info("Task C: Completed.")

def task_d():
    logging.info("Task D: Generating report...")
    time.sleep(5)
    logging.info("Task D: Completed.")

def task_e():
    logging.info("Task E: Finalizing process...")
    time.sleep(2)
    logging.info("Task E: DAG execution completed successfully.")

# Define the DAG
with DAG(
    dag_id='lesson1_sequential_dag',
    default_args=default_args,
    description='A simple sequential DAG with logging and delays',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson1', 'sequential']
) as dag:

    # Define tasks
    task_1 = PythonOperator(
        task_id='task_a',
        python_callable=task_a,
    )

    task_2 = PythonOperator(
        task_id='task_b',
        python_callable=task_b,
    )

    task_3 = PythonOperator(
        task_id='task_c',
        python_callable=task_c,
    )

    task_4 = PythonOperator(
        task_id='task_d',
        python_callable=task_d,
    )

    task_5 = PythonOperator(
        task_id='task_e',
        python_callable=task_e,
    )

    # Define task dependencies
    task_1 >> task_2 >> task_3 >> task_4 >> task_5
