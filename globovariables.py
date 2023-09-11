# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models

from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable


with models.DAG(
        'globomantics_variables',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once') as dag:


    def globo_greeting():
        import logging
        wish = Variable.get("wish")
        logging.info('Hello '+wish)

    hello_globomantics = python_operator.PythonOperator(
        task_id='variable',
        python_callable=globo_greeting)

    t1 = bash_operator.BashOperator(
        task_id='print_date',
        bash_command='echo "date={{ ds }}"',
    )

    # Define the order in which the tasks complete by using the >> and << operators

    hello_globomantics >> t1

