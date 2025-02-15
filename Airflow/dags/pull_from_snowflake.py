from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


# Function to fetch Snowflake connection details
def get_snowflake_connection(conn_id):
    """
    Retrieves Snowflake connection details from Airflow connections.
    """
    conn = BaseHook.get_connection(conn_id)
    return {
        'account': conn.extra_dejson.get('account'),
        'user': conn.login,
        'password': conn.password,
        'warehouse': conn.extra_dejson.get('warehouse'),
        'database': conn.extra_dejson.get('database'),
        'schema': conn.schema,
        'role': conn.extra_dejson.get('role')
    }


def pull_raw_data_from_snowflake(**context):
    snowflake_conn = get_snowflake_connection("SNOWFLAKE-CONNECTION")

    try:
        with snowflake.connector.connect(
            account=snowflake_conn["account"],
            user=snowflake_conn["user"],
            password=snowflake_conn["password"],
            warehouse=snowflake_conn["warehouse"],
            database=snowflake_conn["database"],
            schema=snowflake_conn["schema"],
            role=snowflake_conn["role"]
        ) as conn:

            cur = conn.cursor()

            sql_query = "SELECT C_CUSTKEY, C_NAME FROM CUSTOMER LIMIT 10"

            cur.execute(sql_query)

            result = cur.fetchall()

            logging.info(f"Cursor description: {cur.description}")
            logging.info(f"First element Cursor description: {[desc[0] for desc in cur.description]}")

            column_names = [desc[0] for desc in cur.description]

            df = pd.DataFrame(result, columns=column_names)

            logging.info(f"Dataframe from Snowflake: {df}")

            context["task_instance"].xcom_push(key="sample_data", value=df.to_json())

    except Exception as e:
        logging.info(str(e))


def send_data_to_snowflake(**context):
    df_json = context["task_instance"].xcom_pull(task_ids="pull_raw_data_from_snowflake", key="sample_data")

    df = pd.read_json(df_json)

    snowflake_conn = get_snowflake_connection("SNOWFLAKE-CONNECTION-PUSH")

    try:
        with snowflake.connector.connect(
            account=snowflake_conn["account"],
            user=snowflake_conn["user"],
            password=snowflake_conn["password"],
            warehouse=snowflake_conn["warehouse"],
            database=snowflake_conn["database"],
            schema=snowflake_conn["schema"],
            role=snowflake_conn["role"]
        ) as conn:

            success, nchunks, nrows, _ = write_pandas(
                conn,
                df,
                "SAMPLED_CUSTOMERS",
                auto_create_table=True,
                overwrite=False
            )

            if success:
                logging.info(f"Successfullly uploaded {nrows} in {nchunks} chunks to SAMPLED_CUSTOMERS in Snowflake")
            else:
                logging.info("FAILED")


    except Exception as e:
        logging.info(str(e))



# Define the DAG
with DAG(
    dag_id='pull_from_snowflake',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description="A DAG that processes raw employee data and uploads processed data to Snowflake."
) as dag:

    # Task 1: Pull Raw Data from Snowflake
    pull_raw_data_task = PythonOperator(
        task_id='pull_raw_data_from_snowflake',
        python_callable=pull_raw_data_from_snowflake
    )

    send_data_to_snowflake = PythonOperator(
        task_id="send_data_to_snowflake",
        python_callable=send_data_to_snowflake
    )


    pull_raw_data_task >> send_data_to_snowflake