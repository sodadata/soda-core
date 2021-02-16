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
scan_builder.scan_yml_file = 'tables/my_table.yml'
# scan_builder will automatically find the warehouse.yml in the parent and same directory as the scan YAML file
# scan_builder.warehouse_yml_file = '../warehouse.yml'
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

Programmatic scan execution using dicts:

```python
scan_builder = ScanBuilder()
scan_builder.warehouse_yml_dict = {
    'name': 'my_warehouse_name',
    'connection': {
        'type': 'snowflake',
        ...
    }
}
scan_builder.scan_yml_dict = {
    ...
}
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```
