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
    'retries': 0, 
}


# Function to log start of execution
def start_execution():
    logging.info("Starting DAG Execution...")


# Function to simulate a task that has a 50% chance of failing
def risky_task(task_name):
    if random.random() < 0.5:  # 50% chance to fail
        raise Exception(f"{task_name} failed! Retrying...")
    logging.info(f"{task_name} executed successfully.")


# Task Functions with Simulated Failures
def task_a1():
    risky_task("Task A1")

def task_a2():
    risky_task("Task A2")

def task_b1():
    risky_task("Task B1")

def task_b2():
    risky_task("Task B2")

# Function to log final execution
def final_task():
    logging.info("Final Task executed after both paths completed (success or fail).")


# Define the DAG
with DAG(
    dag_id='third_tutorial_c',
    default_args=default_args,
    description='A DAG demonstrating parallel execution with failure handling',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson3', 'branching', 'bypass_failure']
) as dag:

    # Start Task
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_execution,
    )

    # Path A (Tasks that may fail)
    task_a1_op = PythonOperator(
        task_id='task_a1',
        python_callable=task_a1,
    )

    task_a2_op = PythonOperator(
        task_id='task_a2',
        python_callable=task_a2,
    )

    # Path B (Tasks that may fail)
    task_b1_op = PythonOperator(
        task_id='task_b1',
        python_callable=task_b1,
    )

    task_b2_op = PythonOperator(
        task_id='task_b2',
        python_callable=task_b2,
    )

    # Final Converging Task (Runs even if Path B fails)
    final_task_op = PythonOperator(
        task_id='final_task',
        python_callable=final_task,
        trigger_rule='all_done',  # Ensures execution after both paths finish (success or fail)
    )

    # Define Task Dependencies
    start_task >> [task_a1_op, task_b1_op]  # Both paths start at the same time
    task_a1_op >> task_a2_op
    task_b1_op >> task_b2_op
    [task_a2_op, task_b2_op] >> final_task_op  # final_task waits for both paths
