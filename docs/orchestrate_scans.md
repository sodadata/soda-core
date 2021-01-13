# Orchestrate scans

This section explains how to run scans as part of your data pipeline and
how to use Soda SQL to, when necessary, stop the pipeline to prevent bad data from flowing downstream.

Soda SQL is build in such a way that it's easy to include it as a step in your
pipeline orchestration.

Depending on your desired setup you can configure your orchestration tool to run a soda scan as blocking
(for testing) or in parallel (for monitoring).

## Programmatic scans

Here's how to run scans using Python:

Programmatic scan execution based on default directory structure:
```python
scan_builder = ScanBuilder()
scan_builder.read_scan_from_dirs('~/my_warehouse_dir', 'my_table_dir')
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

> If you're reading this and thinking: "I want to contribute!", let us know! Fork the
[repository on Github](https://github.com/sodadata/soda-sql/fork), open up a pull request and
[let us know](https://github.com/sodadata/soda-sql/discussions) you're working on it.

## Other orchestration solutions

TODO: describe how to run a soda scan in orchestration tools like:

* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

> If you're reading this and thinking: "I want to contribute!", let us know! Fork the
[repository on Github](https://github.com/sodadata/soda-sql/fork), open up a pull request and
[let us know](https://github.com/sodadata/soda-sql/discussions) you're working on it.