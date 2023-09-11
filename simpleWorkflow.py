# Import Definition
import datetime
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator

# DAG Definition

with models.DAG(
        'globomantics_simple_workflow',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='*/10 * * * *') as dag:

    # A simple Python function
    def globo_greeting():
        import logging
        logging.info('Hello Globomantics!')

    # hello_globomantics task calls the "globo_greeting" Python function.
    hello_globomantics = python_operator.PythonOperator(
        task_id='hello',
        python_callable=globo_greeting)

    # Define the task thet needs to be executed

    hello_globomantics