# [START composer_simple_define_dag]
import datetime
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.utils.task_group import TaskGroup

# Define a DAG (directed acyclic graph) of tasks.

with models.DAG(
        'acgc-lab01',
        start_date=airflow.utils.dates.days_ago(0),
        schedule_interval='@once') as dag:

    def start_message():
        import logging
        logging.info('Starting a SubGroup Task...')

    start = python_operator.PythonOperator(
        task_id='task01',
        python_callable=start_message)

    with TaskGroup('group_tasks') as group_tasks:
        t2 = bash_operator.BashOperator(
            task_id='task03',
            bash_command='echo "I am a subtask with taskid task03"',
        )
        t3 = bash_operator.BashOperator(
            task_id='task04',
            bash_command='echo "I am a sub task with taskid task04"',
        )

    end = bash_operator.BashOperator(
        task_id='task04',
        bash_command='echo "End of SubGroup Task"',
    )

    # Define the order in which the tasks complete by using the >> and << operators

    start >> group_tasks >> end