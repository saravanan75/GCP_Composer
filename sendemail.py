# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.operators.email import EmailOperator
# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'globomantics_sendemail',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once') as dag:

    def globo_greeting():
        import logging
        logging.info('Hello Globomantics!')

    hello_globomantics = python_operator.PythonOperator(
        task_id='templates',
        python_callable=globo_greeting)

    t1 = bash_operator.BashOperator(
        task_id='print_date',
        bash_command='echo "date={{ ds }}"',
    )


    task_email = EmailOperator(
        task_id="sendemail",
        conn_id="sendgrid_default",
        # You can specify more than one recipient with a list.
        to="acgc.1302@gmail.com",
        subject="EmailOperator test for SendGrid",
        html_content="This is a test message sent through SendGrid.",
        dag=dag,
    )

    # Define the order in which the tasks complete by using the >> and << operators

    hello_globomantics >> t1 >> task_email