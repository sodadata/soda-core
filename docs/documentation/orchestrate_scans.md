---
layout: default
title: Configure orchestrated scans
parent: Documentation
nav_order: 10
---

# Configure orchestrated scans

Integrate Soda SQL with a **data orchestration tool** such as, Airflow, Dagster, or dbt, to automate and schedule your search for "bad" data. 

Not only can you schedule [scans]({% link documentation/glossary.md %}#scan) of [warehouse]({% link documentation/glossary.md %}#warehouse) [tables]({% link documentation/glossary.md %}#table), you can also configure actions that the orchestration tool can take based on scan output. For example, if the output of a scan reveals a large number of failed tests, the orchestration tool can automatically block "bad" data from contaminating your data pipeline. 

Use the results of a Soda SQL scan to instruct your orchestration tool to:
- block "bad" data from entering your data pipeline (for testing), or,
- allow data to enter the pipeline (for monitoring).

![orchestrate](../assets/images/orchestrate.png){:height="800px" width="800px"}

Follow the instructions that correspond to your data orchestration tool:

[Apache Airflow using PythonVirtualenvOperator](#apache-airflow-using-pythonvirtualenvoperator)<br />
[Apache Airflow using PythonOperator](#apache-airflow-using-pythonoperator)<br />
[Apache Airflow using BashOperator](#apache-airflow-using-bashoperator)<br />
More coming soon
<br />


## Apache Airflow using PythonVirtualenvOperator

Though you can install Soda SQL directly in your Airflow environment, the instructions below use PythonVirtualenvOperator to run Soda SQL scans in a different environment. This keeps Soda SQL software separate to prevent any dependency conflicts. 

1. Install `virtualenv` in your main Airflow environment.
2. Set the following variables in your Airflow environment.
```python
warehouse_yml = Variable.get('soda_sql_warehouse_yml_path')
scan_yml = Variable.get('soda_sql_scan_yml_path')
```
3. Configure as per the following example.

```python
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'soda_sql',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_soda_scan(warehouse_yml_file, scan_yml_file):
    from sodasql.scan.scan_builder import ScanBuilder
    scan_builder = ScanBuilder()
    # Optionally you can directly build the warehouse dict from Airflow secrets/variables
    # and set scan_builder.warehouse_dict with values.
    scan_builder.warehouse_yml_file = warehouse_yml_file
    scan_builder.scan_yml_file = scan_yml_file
    scan = scan_builder.build()
    scan_result = scan.execute()
    if scan_result.has_test_failures():
        failures = scan_result.get_test_failures_count()
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

## Apache Airflow using PythonOperator

If you do not want to use a PythonVirtualenvOperator, which installs Soda SQL on invocation, you can use PythonOperator. 
 
1. Set the following variables in your Airflow environment.
```python
warehouse_yml = Variable.get('soda_sql_warehouse_yml_path')
scan_yml = Variable.get('soda_sql_scan_yml_path')
```
2. Configure as per the following example.

```python
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

```

## Apache Airflow using BashOperator

Invoke a Soda SQL scan using Airflow BashOperator.

1. Install Soda SQL in your Airflow environment. 
2. Set the following variables in your Airflow environment.
```python
warehouse_yml = Variable.get('soda_sql_warehouse_yml_path')
scan_yml = Variable.get('soda_sql_scan_yml_path')
```
3. Configure as per the following example.

```python
# In this  DAG, the `soda_sql_scan_demodata` task fails when the tests you
# defined for the `demodata` table fail. This prevents the `publish_data_op` from
# running. You can further customize the bash command to use different Soda SQL command
# options, such as passing variables to the `soda scan` command.

from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Use the same variable name that you used when you set your Airflow variables
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

# Soda SQL Scan which runs the appropriate table scan for the ingestion
soda_sql_scan_op = BashOperator(
    task_id='soda_sql_scan_demodata',
    bash_command=f'cd {soda_sql_project_path} && soda scan warehouse.yml tables/demodata.yml',
    dag=dag
)

# A dummy operator to simulate data publication when the Soda SQL Scan task is successful
publish_data_op = DummyOperator(
    task_id='publish_data'
)

ingest_data_op >> soda_sql_scan_op >> publish_data_op
```

## Go further

- If you want to write integration instructions for your favorite data orchestration tool, please contribute to our open-source docs! [Post a note on GitHub](https://github.com/sodadata/soda-sql/discussions) to let us know your plans.
- Need help? [Post your questions on GitHub](https://github.com/sodadata/soda-sql/discussions)
or [join our Slack community](https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg)
- Learn how to configure [programmatic Soda SQL scans]({% link documentation/programmatic_scan.md %}).
