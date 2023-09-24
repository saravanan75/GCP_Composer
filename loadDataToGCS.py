# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.configuration import conf
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'mv_gcs_to_gcs',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once') as dag:

    mv_gcs_gcs = GCSToGCSOperator(
        task_id="move_to_gcs",
        source_bucket="gs_acg_source",
        source_object="data/products.csv",
        destination_bucket="gs_acg_analytics",
        destination_object="products.csv",
        exact_match=True,
    )

mv_gcs_gcs