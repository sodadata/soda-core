---
layout: default
title: Programmatic scan
parent: Documentation
nav_order: 10
---

# Programmatic scan

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
