---
layout: default
title: Filtering
parent: Documentation
nav_order: 8
---

# Filtering

This section explains how to apply dynamic filters on the data in a scan.  Filtering can be
used to run the scan on for example a single time partition of the table or on a country.

Time partitioning requires 1 (or more) columns to filter on.

Let's use this `CUSTOMER_TRANSACTIONS` table as an example:

```sql
CREATE TABLE CUSTOMER_TRANSACTIONS (
  ID VARCHAR(255),
  NAME VARCHAR(255),
  SIZE INT,
  DATE DATE,
  FEEPCT VARCHAR(255),
  COUNTRY VARCHAR(255)
);
```

The `CUSTOMER_TRANSACTIONS` table has a `DATE` column.  Each day new customer transaction
rows are added, but we want our scans to only run on the transactions of the last day. In order
to do so we can add a `filter` to our Scan YAML file:

{% raw %}
```yaml
table_name: CUSTOMER_TRANSACTIONS
filter: "date = DATE '{{ date }}'"
metrics: ...
columns: ...
```
{% endraw %}

Variables are replaced as text.  So don't forget to include quotes in your filter template if needed.

The filter is automatically added to the `WHERE` class of Soda SQL generated queries.

> NOTE: Soda SQL will not add this filter to SQL metric queries.  You have to ensure that this filter logic is also
> part of the queries in your SQL metrics. See [SQL metrics]({% link documentation/sql_metrics.md %}#sql-metrics) to learn more.

As you can see we've used a variable called `date` in our example. This variable can be populated
by supplying an additional `-v` argument to each `soda scan`.

```
soda scan -v date=2021-01-12 warehouse.yml tables/customer_transactions.yml
```

> _Note: CLI does not yet support variables, but this will be made available soon. In the meantime you can use  the 'programmatic style' below_

You can also use variables when invoking a Soda SQL Scan programmatically:

```python
scan_builder = ScanBuilder()
scan_builder.warehouse_yml_file = 'warehouse.yml'
scan_builder.scan_yml_file = 'tables/my_table.yml'
scan_builder.variables = {
    'date': '2021-06-12'
}
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

> For time partitioned tables it makes sense to run a `soda scan` on both
the time partitioned and full table's data.  To achieve this, we recommend
you to create 2 separate Scan YAML files for the same table.
It's a good practice to add `_tp` as the suffix of one of your Scan YAML files
to indicate that  it's a "Time Partitioned" table configuration.