# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.configuration import conf
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'mv_gcs_to_bq',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once') as dag:

    mv_gcs_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket='gs_acg_analytics',
        source_objects=['products.csv'],
        destination_project_dataset_table="globomantics.products",
        schema_fields=[
            {'name': 'sku', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

mv_gcs_bq