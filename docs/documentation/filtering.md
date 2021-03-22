---
layout: default
title: Apply filters
parent: Documentation
nav_order: 8
---

# Apply filters

To test specific portions of data for quality, you can apply dynamic **filters** when you [scan]({% link documentation/glossary.md %}#scan) data in your [warehouse]({% link documentation/glossary.md %}#warehouse). To do so, you define a filter [configuration key]({% link documentation/glossary.md %}#configuration-key) in your [scan YAML]({% link documentation/glossary.md %}#scan-yaml) file, then add a variable to the `soda scan` command that specifies a portion of data for Soda SQL to scan instead of scanning a larger data set. When you add a variable, Soda SQL adds a filter to the `WHERE` clause of the SQL queries it creates to scan your data. Refer to [How Soda SQL works]({% link documentation/concepts.md %}) to learn more.

For example, where a `CUSTOMER_TRANSACTIONS` [table]({% link documentation/glossary.md %}#table) has a `DATE` column, you may wish to run a scan only against the newest data added to the table. In such a case, you can apply a filter for a specific date so that Soda SQL only scans data associated with that date. 

```shell
soda scan -v date=2021-01-12 warehouse.yml tables/customer_transactions.yml
```

Similarly, if the table has a column for `COUNTRY`, you can apply a filter that scans only the data that pertains to the country you specify as a variable in the `soda scan` command.

```shell
soda scan -v country=FRA warehouse.yml tables/customer_transactions.yml
```


## Configure a filter

1. Open the [scan YAML]({% link documentation/scan.md %}) file associated with the table on which you want to run filtered scans. The scan YAML files are usually located in the [warehouse directory]({% link documentation/glossary.md %}#warehouse-directory) of your Soda SQL project.
2. To the file, add a filter configuration key as per the following example. Be sure to use quotes for input that is in text format.
```yaml
table_name: CUSTOMER_TRANSACTIONS
filter: "date = DATE '{{ date }}'"
metrics: ...
columns: ...
```
3. When you define a variable in your scan YAML file, Soda SQL applies the filter to all tests *except* tests defined in SQL metrics. To apply a filter to SQL metrics tests, be sure to explicitly define the variable in your SQL query. Refer to [Variables in SQL metrics]({% link documentation/sql_metrics.md %}#variables-in-sql-metrics)
4. Save the changes to the YAML file, then run a filtered scan by adding a variable to your `soda scan` command in your command-line interface.
```shell
soda scan -v date=2021-01-12 warehouse.yml tables/customer_transactions.yml
```


For time-partitioned tables, consider configuring a separate scan YAML file for scans that use a filter. For example:
* `customer_transactions.yml` with no filter defined in the file, so you can scan all the data in the table without applying a date filter
```shell
soda scan warehouse.yml tables/customer_transactions.yml
```
* `customer_transactions_tp.yml` with a date filter defined, so you can add a date variable to your `soda scan` command to scan against a date you specify
```shell
soda scan -v date=2021-01-12 warehouse.yml tables/customer_transactions_tp.yml
```


## Configure a filter in a programmatic scan

Alternatively, you can use variables when programmatically invoking a Soda SQL scan. Refer to [Configure programmatic scans]({% link documentation/programmatic_scan.md %}).

1. Follow steps 1 - 3 above to prepare your scan YAML file.
2. Configure your programmatic scan to include variables as per the following example.

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

## Go further

* Learn more about configuring your [scan YAML]({% link documentation/scan.md %}) file.
* Learn more about [how Soda SQL works]({% link documentation/concepts.md %}) file.