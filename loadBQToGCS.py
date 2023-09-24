# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.configuration import conf
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'mv_bq_to_gcs',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once',
        tags=['BQtoGCS']) as dag:

    mv_bq_gcs = BigQueryToGCSOperator(
           task_id="bq_to_gcs",
           destination_cloud_storage_uris='gs://gs_acg_reporting/products.csv',
           source_project_dataset_table='globomantics-395503.globomantics.dest_products',
           export_format='CSV',
    )

mv_bq_gcs