---
layout: default
title: Orchestrate scans
parent: Documentation
nav_order: 9
---

# Orchestrate scans

This section explains how to run scans as part of your data pipeline and
stop the pipeline when necessary to prevent bad data flowing downstream.

Soda SQL is build in such a way that it's easy to run it as a step in your
pipeline orchestration.

Use your orchestration tool to configure if the soda scan should be blocking the pipeline
(for testing) or run in parallel (for monitoring).

## Airflow

There are several options to run Soda SQL with Airflow. We recommend using PythonVirtualenvOperator
since it keeps Soda SQL separate and helps you prevent any dependency conflicts.

###  Using PythonVirtualenvOperator

If you can't install soda-sql to your Ariflow environemnt, you can use
PythonVirtualenvOperator to run Soda scan in a different environment. Please
make sure that you have `virtualenv` installed in your main Airflow environment.


```python
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


# Make sure that this variable is set in your Airflow
warehouse_yml = Variable.get('soda_sql_warehouse_yml_path')
scan_yml = Variable.get('soda_sql_scan_yml_path')

default_args = {
    'owner': 'soda_sql',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_soda_scan(warehouse_yml_file, scan_yml_file):
    from sodasql.scan.scan_builder import ScanBuilder
    scan_builder = ScanBuilder()
    # Optionally you can directly build the Warehouse dict from Airflow secrets/variables
    # and set scan_builder.warehouse_dict with values.
    scan_builder.warehouse_yml_file = warehouse_yml_file
    scan_builder.scan_yml_file = scan_yml_file
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_failures():
        failures = scan_result.failures_count()
        raise ValueError(f"Soda Scan found {failures} errors in your data!")


dag = DAG(
    'soda_sql_python_venv_op',
    default_args=default_args,
    description='A simple Soda SQL scan DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

ingest_data_op = DummyOperator(
    task_id='ingest_data'
)

soda_sql_scan_op = PythonVirtualenvOperator(
    task_id='soda_sql_scan_demodata',
    python_callable=run_soda_scan,
    requirements=["soda-sql==2.0.0b10"],
    system_site_packages=False,
    op_kwargs={'warehouse_yml_file': warehouse_yml,
               'scan_yml_file': scan_yml},
    dag=dag
)

publish_data_op = DummyOperator(
    task_id='publish_data'
)

ingest_data_op >> soda_sql_scan_op >> publish_data_op

```

## Other Options

If you don't want to use a PythonVirtualenvOperator, which will install soda-sql on invocation, you can use normal
PythonOperator. See the example dag under [airflow_python_op.py](../../examples/airflow_python_op.py)

You can check the file [airflow_bash.py](../../examples/airflow_bash.py) on how to create a dag using BashOperator,
and if you want to use a virtualenv with BashOperator, check [airflow_bash_env.py](../../examples/airflow_bash_env.py)

### SodaScanOperator -- Coming Soon!

We are currently working on a custom Soda SQL operator to provide even tighter
integration with Airflow.

Need more help? [Post your questions on GitHub](https://github.com/sodadata/soda-sql/discussions)
or [join our Slack community](https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg)

## Other orchestration solutions

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions) to let
others know you're starting on this.

TODO: describe how to run Soda scans in orchestration tools like

* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions) to let others know
you're starting on this.
