---
layout: default
title: Configure programmatic scans
parent: Documentation
nav_order: 9
---

# Configure programmatic scans

To automate the search for "bad" data, you can use the **Soda SQL Python library** to programmatically execute [scans]({% link documentation/glossary.md %}#scan). 

Based on a set of conditions or a specific event schedule, you can instruct Soda SQL to automatically scan a [warehouse]({% link documentation/glossary.md %}#warehouse) [table]({% link documentation/glossary.md %}#table) for “bad” data. For example, you may wish to scan your data at several points along your data pipeline, perhaps when new data enters a warehouse, after it is transformed, and before it is exported to another warehouse.

Execute a programmatic scan based on Soda SQL's default directory structure:

```python
scan_builder = ScanBuilder()
scan_builder.scan_yml_file = 'tables/my_table.yml'
# scan_builder automatically finds the warehouse.yml in the parent directory of the scan YAML file
# scan_builder.warehouse_yml_file = '../warehouse.yml'
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

Execute a programmatic scan using dicts:

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

## Go further

- Learn more about [How Soda SQL works]({% link documentation/concepts.md %}).
- Learn more about [Warehouse]({% link documentation/warehouse.md %}) and [Scan]({% link documentation/scan.md %}) YAML files.
- Learn how to integrate Soda SQL with a [data orchestration tool]({% link documentation/orchestrate_scans.md %}).
- Need help? [Post your questions on GitHub](https://github.com/sodadata/soda-sql/discussions)
or [join our Slack community](https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg)