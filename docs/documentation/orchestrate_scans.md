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

## Programmatic scans

Here's how to run scans using Python:

Programmatic scan execution based on default dir structure:
```python
scan_builder = ScanBuilder()
scan_builder.read_scan_dir('~/my_warehouse_dir', 'my_table_dir')
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

Programmatic scan execution reading yaml files by path:
```python
scan_builder = ScanBuilder()
scan_builder.read_warehouse_yml('./anydir/warehouse.yml')
scan_builder.read_scan_yml('./anydir/scan.yml')
scan_builder.read_sql_metrics_from_dir('./anydir/')
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

Programmatic scan execution using dicts:
```python
scan_builder = ScanBuilder()
scan_builder.warehouse_dict({
    'name': 'my_warehouse_name',
    'connection': {
        'type': 'snowflake',
        ...
    }
})
scan_builder.scan_dict({...})
scan_builder.sql_metric_dict({...})
scan_builder.sql_metric_dict({...})
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

## Airflow

TODO: describe how to run Soda scans in Airflow.

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions/new?title=Contributing%20Airflow) to let
others know you're starting on this.

## Other orchestration solutions

TODO: describe how to run Soda scans in orchestration tools like

* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions/new) to let others know
you're starting on this.