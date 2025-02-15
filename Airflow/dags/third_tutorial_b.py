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
    'retries': 2,  # Will retry failed tasks twice
    'retry_delay': timedelta(minutes=1),
}

# Function to log start of execution
def start_execution():
    logging.info("Starting DAG Execution...")


# Function to decide which branch to take (both paths will execute)
def decide_branch():
    return ["task_a1", "task_b1"]  # Both branches execute


# Function to simulate failure (50% chance)
def task_a1():
    if random.random() < 0.5:  # 50% chance to fail
        raise Exception("Task A1 failed! Retrying...")
    logging.info("Task A1 executed successfully.")

def task_a2():
    if random.random() < 0.5:
        raise Exception("Task A2 failed! Retrying...")
    logging.info("Task A2 executed successfully.")

def task_b1():
    if random.random() < 0.5:
        raise Exception("Task B1 failed! Retrying...")
    logging.info("Task B1 executed successfully.")

def task_b2():
    if random.random() < 0.5:
        raise Exception("Task B2 failed! Retrying...")
    logging.info("Task B2 executed successfully.")

# Function to log final execution
def final_task():
    logging.info("Final Task executed after both paths succeeded.")

# Define the DAG
with DAG(
    dag_id='third_tutorial_b',
    default_args=default_args,
    description='A DAG demonstrating branching with required success on both paths',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['lesson3', 'branching', 'failure_handling']
) as dag:

    # Start Task
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start_execution,
    )

    # Branching Task (Executes both paths)
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=decide_branch,
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

    # Final Converging Task (Runs only if both paths succeed)
    final_task_op = PythonOperator(
        task_id='final_task',
        python_callable=final_task,
        trigger_rule='all_success',  # Runs only if all upstream tasks succeed
    )

    # Define Task Dependencies
    start_task >> branch_task
    branch_task >> [task_a1_op, task_b1_op]
    task_a1_op >> task_a2_op
    task_b1_op >> task_b2_op
    [task_a2_op] >> final_task_op  # Both must succeed before final_task runs
