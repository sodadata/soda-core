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

There are several options to run Soda SQL with Airflow, the simplest way is to
install Soda SQL in the same environment as your Airflow and invoke `soda scan`
using Airflow BashOperator.

When there are test failures in soda scan, the exit code will be non-zero.

To create a BashOperator, first create a variable in airflow to point to your
Soda SQL project. You can do this either using Airflow web admin ui or via
commandline:

```bash
airflow variables set "soda_sql_project_path" "YOUR_SODA_SQL_PROJECT_LOCATION"
```

Take a note of the variable name and use it in your DAG. Here is a sample DAG
that uses Airflow DummyOperators to denote data ingestion task and publishing
task:

```python
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
    bash_command=f'cd {soda_sql_project_path} && soda scan warehouse.yml tables/demodata.yml',
    dag=dag
)

# A dummy operator to simulate data publication when the Soda SQL Scan task is successful
publish_data_op = DummyOperator(
    task_id='publish_data'
)

ingest_data_op >> soda_sql_scan_op >> publish_data_op

```

In the above DAG, `soda_sql_scan_demodata` task will fail when the tests you
defined for `demodata` table fails. This will prevent the `publish_data_op` from
running. You can further customize the bash command to use different soda scan command
options, for example passing variables to `soda scan` command.

We are currently working on a custom SodaSQL operator to provide even tighter
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
