# Time partitioning

This section explains how to run metrics and tests on a specific time partition
of the table.

Time partitioning requires 1 (or more) columns which reflect the time or date.
It allows you to to run a scan on a partition of your data which corresponds to the
defined time period.

Let's use this `CUSTOMER_TRANSACTIONS` table as an example:

```
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

In the `scan.yml`, add the following `time_filter`:

```
table_name: CUSTOMER_TRANSACTIONS
time_filter: "date = DATE '{{ date }}'"
metrics: ...
columns: ...
```

The defined `time_filter` is added as a `WHERE` clause to each SQL query.

In our `time_filter` query we've made use of a variable: `date`. The value of this variable
can be passed to the scan on the command line like:

> _Note, coming soon: Soda CLI does not yet support variables. In the meantime you can use the programmatic style
as shown below_
```
soda scan -v date=2021-01-12 ./sales_snowflake customer_transactions
```

And programmatically, variables can be passed to a scan like this:
```
scan_builder = ScanBuilder()
scan_builder.read_scan_from_dirs('~/my_warehouse_dir', 'my_table_dir')
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    print('Scan has test failures, stop the pipeline')
```

For time partitioned tables, it makes sense to run scans on both
the partitioned as well as on the full table. You can achieve this by
creating 2 separate table directories, each having their own `scan.yml`.

It's good practice to add `_tp` as the suffix to the table
directory to indicate that it's a "Time Partitioned" table configuration.
