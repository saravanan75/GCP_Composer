# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Define a DAG (directed acyclic graph) of tasks.

QUERY = (
    "SELECT name, quantity FROM globomantics-395503.globomantics.products;"
)
with models.DAG(
        'query_bq_data',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once',
        tags=['copyBQ']) as dag:

    run_query_bq = BigQueryInsertJobOperator(
           task_id="query_bq",
           configuration={
                "query": {
                    "query": QUERY,
                    "use_legacy_sql": False,
                },
                "destinationTable": {
                    "projectId": "globomantics-395503",
                    "datasetId": "globomantics",
                    "tableId": "dest_products"
                },
                "write_disposition": 'WRITE_TRUNCATE',
           },
    )

run_query_bq