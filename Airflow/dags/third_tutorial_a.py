from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
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


# Function to decide which branch to take
def decide_branch(**kwargs):
    decision = random.choice(["path_a", "path_b"])  # Randomly choose a path
    logging.info(f"Branch decision: {decision}")
    
    if decision == "path_a":
        return "task_a1"
    else:
        return "task_b1"


# Task Functions
def task_a1():
    logging.info("Executing Task A1...")

def task_a2():
    logging.info("Executing Task A2...")

def task_b1():
    logging.info("Executing Task B1...")

def task_b2():
    logging.info("Executing Task B2...")

def final_task():
    logging.info("Executing Final Task after branching...")


# Define the DAG
with DAG(
    dag_id='third_tutorial_a',
    default_args=default_args,
    description='A DAG demonstrating branching (diverging & converging)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson3', 'branching']
) as dag:

    # Start Task
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: logging.info("Starting DAG Execution..."),
    )

    # Branching Task
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_branch,
        provide_context=True,
    )

    # Path A
    task_a1 = PythonOperator(
        task_id='task_a1',
        python_callable=task_a1,
    )

    task_a2 = PythonOperator(
        task_id='task_a2',
        python_callable=task_a2,
    )

    # Path B
    task_b1 = PythonOperator(
        task_id='task_b1',
        python_callable=task_b1,
    )

    task_b2 = PythonOperator(
        task_id='task_b2',
        python_callable=task_b2,
    )

    # Final Converging Task
    final_task = PythonOperator(
        task_id='final_task',
        python_callable=final_task,
        trigger_rule='none_failed_min_one_success',  # Ensures execution after any path
    )

    # Define Task Dependencies
    start_task >> branch_task
    branch_task >> [task_a1, task_b1]
    task_a1 >> task_a2 >> final_task
    task_b1 >> task_b2 >> final_task
