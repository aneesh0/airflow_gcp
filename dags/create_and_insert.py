from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.bigquery_operator import BigQueryCreateEmptyTableOperator, BigQueryOperator

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
    dag_id='create_and_insert_table',
    default_args=default_args,
    description='DAG to create empty table, dataset provided',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

create_table_task = BigQueryCreateEmptyTableOperator(

    project_id="dark-furnace-298806",
    dataset_id="shtest",
    task_id="create_empty_table",
    table_id="new_service_table",
    bigquery_conn_id="bigquery_default",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dag

)

insert_task = BigQueryOperator(
    task_id="insert_into_table",
    bql=''' 
    select * from 
    bigquery-public-data.austin_311.311_service_requests
    limit 5 
     ''',
    use_legacy_sql=False,
    destination_dataset_table="dark-furnace-298806:shtest.new_service_table",
    write_disposition="WRITE_EMPTY",
    bigquery_conn_id="bigquery_default",
    dag=dag
)

create_table_task >> insert_task
