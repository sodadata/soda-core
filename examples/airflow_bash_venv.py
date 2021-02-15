# Using your favorite virtualenv management tool create a venv, we will use
# `virtualenv` command as example. Please review soda-sql installation
# requirements to use tested python version for creating the virtualenv.
#
# create a new virtualenv in a convenient location
# `virtualenv .sodavenv && .sodavenv/bin/pip install soda-sql`
#
# Now you can modify the BashOperator in the above DAG as follows:
#

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Use the same variable name that you used in airflow variable creation
soda_sql_project_path = Variable.get('soda_sql_project_path')

default_args = {
    'owner': 'soda_sql',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'soda_sql_scan',
    default_args=default_args,
    description='A simple Soda SQL scan DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)
# A dummy operator to simulate data ingestion
ingest_data_op = DummyOperator(
    task_id='ingest_data'
)

# Soda SQL Scan which will run the appropriate table scan for the ingestion
soda_sql_scan_op = BashOperator(
    task_id='soda_sql_scan_demodata',
    bash_command=f'cd {soda_sql_project_path} && .sodavenv/bin/soda scan warehouse.yml tables/demodata.yml',
    dag=dag
)

# A dummy operator to simulate data publication when the Soda SQL Scan task is successful
publish_data_op = DummyOperator(
    task_id='publish_data'
)

ingest_data_op >> soda_sql_scan_op >> publish_data_op
