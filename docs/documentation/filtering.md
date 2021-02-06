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

The `CUSTOMER_TRANSACTIONS` has a `DATE` column.  Each day new customer transaction
rows are added.  After they are added the goal is to run the scan on the customer
transactions of the last day.

In the `scan.yml`, add a `filter` like this:

```yaml
table_name: CUSTOMER_TRANSACTIONS
filter: "date = DATE '\{\{ date \}\}'"
metrics: ...
columns: ...
```

The time filter is added to the SQL queries in the where clause.

The `date` can be passed to the scan as a variable on the command line like:

> _Note: CLI does not yet support variables. Coming soon.  Use programmatic style below_
```
soda scan -v date=2021-01-12 ./sales_snowflake customer_transactions
```

And programmatically, variables can be passed to a scan like this:
```python
scan_builder = ScanBuilder()
scan_builder.scan_yml_file = 'tables/my_table.yml'
scan_builder.variables = {
  'date': '2021-06-12'
}
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

For time partitioned tables, it makes sense to measure and test on both
the time partitions and on the full table.  To achieve this, we recommend
that you create 2 separate table dirs for it each having a `scan.yml`.
It's a good practice to add `_tp` as the suffix to the table
directory to indicate it's a "Time Partitioned" table configuration.