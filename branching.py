# [START composer_simple_define_dag]
import datetime
import airflow
import logging
import random
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'globomantics_branching',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='*/10 * * * *') as dag:

    def business_logic():
        returncode = random.randint(1,3)
        logging.info("The return code is ",returncode)
        if returncode > 1:
            return 'print_failure'
        return 'print_date'

    def globo_greeting():
        logging.info('Hello Globomantics!')

    hello_globomantics = python_operator.PythonOperator(
        task_id='templates',
        python_callable=globo_greeting)

    t1 = bash_operator.BashOperator(
        task_id='print_date',
        bash_command='echo "Business Logic successful: date={{ ds }}"',
    )

    t2 = bash_operator.BashOperator(
        task_id='print_failure',
        bash_command='echo "Business Logic failed"',
    )

    t3 = BranchPythonOperator(
        task_id='branch',
        python_callable=business_logic
    )



    # Define the order in which the tasks complete by using the >> and << operators

    hello_globomantics >> t3 >> [t1, t2]