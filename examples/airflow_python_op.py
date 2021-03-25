#
# If you installed Soda SQL in your python environment you can  use
# PythonOperator to invoke Soda Scan. The following shows a sample Airflow DAG
# using PythonOperator that you can use as a starting point.
#

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from sodasql.scan.scan_builder import ScanBuilder
from airflow.exceptions import AirflowFailException

# Make sure that this variables are set in your Airflow
warehouse_yml = Variable.get('soda_sql_warehouse_yml_path')
scan_yml = Variable.get('soda_sql_scan_yml_path')

default_args = {
    'owner': 'soda_sql',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_soda_scan(warehouse_yml_file, scan_yml_file):
    scan_builder = ScanBuilder()
    scan_builder.warehouse_yml_file = warehouse_yml_file
    scan_builder.scan_yml_file = scan_yml_file
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_test_failures():
        failures = scan_result.get_test_failures_count()
        raise AirflowFailException(f"Soda Scan found {failures} errors in your data!")


dag = DAG(
    'soda_sql_python_op',
    default_args=default_args,
    description='A simple Soda SQL scan DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

ingest_data_op = DummyOperator(
    task_id='ingest_data'
)

soda_sql_scan_op = PythonOperator(
    task_id='soda_sql_scan_demodata',
    python_callable=run_soda_scan,
    op_kwargs={'warehouse_yml_file': warehouse_yml,
               'scan_yml_file': scan_yml},
    dag=dag
)

publish_data_op = DummyOperator(
    task_id='publish_data'
)

ingest_data_op >> soda_sql_scan_op >> publish_data_op

