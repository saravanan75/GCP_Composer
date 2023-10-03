# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'setup_bq',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once',
        tags=['Lab02']) as dag:

    run_create_ds = BigQueryCreateEmptyDatasetOperator(
           task_id="create_ds",
           project_id="globomantics-395503",
           dataset_id="lab02",
           if_exists="skip",
    )

    run_create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id="globomantics-395503",
        dataset_id="lab02",
        table_id="lab02_table",
        if_exists="skip",
    )

    run_load_table = GCSToBigQueryOperator(
        task_id="load_table",
        bucket='gs_acg_analytics',
        source_objects=['employees.csv'],
        destination_project_dataset_table="lab02.lab02_table",
        schema_fields=[
            {'name': 'firstname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lastname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

run_create_ds >> run_create_table >> run_load_table