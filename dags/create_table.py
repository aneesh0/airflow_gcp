from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator

default_args = {
    "owner": "aneesh",
    "start_date": datetime(2020,12,17),
    "depends_on_past": False,
    "email_on_failiure": False,
    "email_on_retry": False,
    "retries":1,
    "retries_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id='create_table4',
    default_args=default_args,
    description='DAG to create empty table, dataset provided',
    schedule_interval="@once",
    catchup=False,
)

create_table_task = BigQueryCreateEmptyTableOperator(

    project_id="dark-furnace-298806",
    dataset_id="shtest",
    task_id="create_tabel4_task",
    table_id="new_table4",
    bigquery_conn_id="bigquery_default",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dag

)
