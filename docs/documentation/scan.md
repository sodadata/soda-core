---
layout: default
title: Scan YAML
parent: Documentation
nav_order: 5
---

# Scan YAML

A **scan** is a Soda SQL command that uses SQL queries to extract information about data in a database table. Use the ``soda scan`` CLI command to initiate a scan of your data.

Instead of laboriously accessing your database and then manually defining SQL queries to analyze the data in tables, you can use a much simpler Soda SQL scan. First, you configure scan metrics and tests in a **Scan YAML** file, then Soda SQL uses the input from that file to prepare, then run SQL queries against your data.

## Create a Scan YAML file

You need to create a **Scan YAML** file for every table in your database that you want to scan. If you have 20 tables in your database, you need 20 YAML files, each corresponding to a single table. 

You can create Scan YAML files yourself, but the CLI command ``soda analyze`` sifts through the contents of your database and autmatically prepares a Scan YAML file for each table. Soda SQL puts the YAML files in the ``/tables`` directory which is in the same directory as your ``warehouse.yml`` file. In the example below, Soda SQL created a Scan YAML file named ``demodata.yml`` and put it in the ``/tables`` directory.

In your command-line interface, navigate to the directory that contains your ``warehouse.yml`` file, then execute the following command:

```shell
soda analyze
```

Output:

```shell
  | Analyzing warehouse.yml ...
  | Querying warehouse for tables
  | Creating tables directory tables
  | Executing SQL query: 
SELECT table_name 
FROM information_schema.tables 
WHERE lower(table_schema)='public'
  | SQL took 0:00:00.008511
  | Executing SQL query: 
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE lower(table_name) = 'demodata' 
  AND table_catalog = 'sodasql' 
  AND table_schema = 'public'
  | SQL took 0:00:00.013018
  | Executing SQL query: 
  ...
    | SQL took 0:00:00.008593
  | Creating tables/demodata.yml ...
  | Next run 'soda scan warehouse.yml tables/demodata.yml' to calculate measurements and run tests
```

If you decide to create your own Scan YAML files manually, best practice dictates that you name the YAML file using the same name as the table in your database. 

## Anatomy of the Scan YAML file

When it creates your Scan YAML file, Soda SQL pre-populates it with the ``test`` and ``metric`` configurations it deemed useful based on the data in the table it analyzed. You can keep those configurations intact and use them to run your scans, or you can adjust or add to them to fine tune the tests Soda SQL runs on your data.  

The following describes the contents of a Scan YAML file that Soda SQL created and pre-populated.

![scan-anatomy](../assets/images/scan-anatomy.png){:height="440px" width="440px"}


**1** - The value of **table_name** identifies a SQL table in your database. If you were writing a SQL query, it is the value you would supply for ``FROM``.

**2** - A **metric** is a property of the data in your database.  A **measurement** is the value for a metric that Soda SQL obtains during a scan. For example, in ``row_count = 5``, ``row_count`` is the metric and ``5`` is the measurement.

**3** - A **test** is a Python expression that, during a scan, checks for metrics that match the parameters defined for a measurement. As a result of a scan, a test either passes or fails. For example, the test ``row_count > 0`` checks to see if the table has at least one row. If the test passes, it means the table has at least one row; if the test fails, it means the table has no rows, which means that the table is empty. Tests in this part of the YAML file apply to all columns in the table. A single Soda SQL scan can run many tests on the contents of the whole table.

**4** - A **column** identifies a SQL column in your table. Use column names to configure tests against individual columns in the table. A single Soda SQL scan can run many tests in many columns.

**5** - **``id``** and **``feepct``** are column names that identify specific columns in this table. 

**6** - The value for **valid_format** identifies the only form of data in the column that Soda SQL recognizes as valid during a scan. In this case, any row in the ``id`` column that contains data that is UUID format (universally unique identifier) is valid; anything else is invalid.

**7** - Same as #3, except the tests in the ``column`` section of the YAML file run only against the contents of the single, identified column. In this case, the test``invalid_percentage == 0`` checks to see if all rows in the ``id`` column contain data in a valid format. If the test passes, it means that 0% of the rows contain data that is invalid; if the test fails, it means that more than 0% of the rows contain invalid data, which is data that is in non-UUID format. 


# Run a scan

{% include run-a-scan.md %}

## Scan output

By default, the output of a Soda SQL scan appears in your command line interface. In the example below, Soda SQL executed three tests and all the tests passed. The ``Exit code`` is a process code: 0 indicates success with no test failures; a non-zero number indicates failures.

```shell
  | 2.0.0b18
  | Scanning tables/demodata.yml ...
  | Soda cloud: dev.sodadata.io
  | Executing SQL query: 
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE lower(table_name) = 'demodata' 
  AND table_catalog = 'sodasql' 
  AND table_schema = 'public'
  ...
  | < 200 {}
  | 54 measurements computed
  | 3 tests executed
  | All is good. No tests failed.
  | Exiting with code 0
```

Optionally, if you have a **Soda Cloud** account and you have [connected it to the Soda SQL too]({% link documentation/connect_to_cloud.md %}) in your environment, Soda SQL automatically pushes the scan output to your Soda Cloud account. You can log in and view the Monitor Results; each row in the Monitor Results table represents the output of one scan.

Optionally, you can [programmatically insert]({% link documentation/programmatic_scan.md %}) the output of Soda SQL scans into your data orchestration tool such as Dagster or Apache Airflow. In your orchestration tool, you can use Soda SQL scan results to block the pipeline if it encounters bad data, or to run in parallel to surface issues with your data.

## Next

* Learn more about the [warehouse YAML]({% link documentation/warehouse.md %}) file.
* Learn how to [configure metrics]({% link documentation/sql_metrics.md %}) in your YAML files.
* Learn more about [Soda SQL tests]({% link documentation/tests.md %}).
* Learn how to apply [filters]({% link documentation/filtering.md %}) to your scan.
