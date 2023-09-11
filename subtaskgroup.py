# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'globomantics_subtaskgroup',
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

    with TaskGroup('group_tasks') as group_tasks:
        t2 = bash_operator.BashOperator(
            task_id='fetch_file',
            bash_command='echo "Fetching File from Server"',
        )

        with TaskGroup('subgroup1') as subgroup1:
            t3 = bash_operator.BashOperator(
                task_id='cleanup_file',
                bash_command='echo "Cleaning up File..."',
            )
            t4 = bash_operator.BashOperator(
                task_id='format_file',
                bash_command='echo "Formatting File..."',
            )
        with TaskGroup('subgroup2') as subgroup2:
            t3 = bash_operator.BashOperator(
                task_id='format_file',
                bash_command='echo "Formatting File..."',
            )

            t4 = bash_operator.BashOperator(
                task_id='print_file',
                bash_command='echo "Printing File..."',
            )



    # Define the order in which the tasks complete by using the >> and << operators

    hello_globomantics >> group_tasks >> t1