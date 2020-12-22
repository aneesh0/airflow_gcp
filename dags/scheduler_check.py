from airflow import DAG
import logging
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "aneesh",
    "start_date": datetime(2020, 12, 20),
    "depends_on_past": False,
    "email_on_failiure": False,
    "email_on_retry": False,
    "retries": 1,
    "retries_delay": timedelta(minutes=5)
}


def log_to_console():
    logging.info("task executed")


dag = DAG(
    dag_id='scheduler_check',
    default_args=default_args,
    description='DAG to test scheduler working fine',
    schedule_interval="*/15 * * * *",
    catchup=False,
)

log_print_task = PythonOperator(
    task_id="scheduler_logging_task",
    python_callable=log_to_console,
    dag=dag

)
