---
layout: default
title: Glossary
parent: Documentation
nav_order: 16
---

# Glossary
<!--This glossary contains Soda-specific terms only. Do not define industry terminology such as "SQL" or "query".-->

### alert 
A setting that you configure in Soda Cloud by specifying key:value thresholds which, if passed, trigger a notification. See also: [notification](#notification).

### analyze
A Soda SQL CLI command that sifts through the contents of your database and automatically prepares a scan YAML file for each table. See [Create a scan YAML file]({% link documentation/scan.md %}#create-a-scan-yaml-file).

### column
A column in a table in your warehouse.

### column configuration key
The key in the key-value pair that you use to define what qualifies as a valid value in a column. A Soda SQL scan uses the value of a column configuration key to determine if it should pass or fail a test on a column. For example, in `valid_format: UUID` , `valid_format` is a column configuration key and `UUID` is the only format of the data in the column that Soda SQL considers valid. See [Column metrics]({% link documentation/sql_metrics.md %}#column-metrics).

### column metric
A property of the data of a single column in your database. Use a column metric to define tests that apply to specific columns in a table during a scan. See [Column metrics]({% link documentation/sql_metrics.md %}#column-metrics).

### configuration key
The key in the key-value pair that you use to define configuration in your scan YAML file. See [Scan YAML configuration keys]({% link documentation/scan.md %}#scan-yaml-configuration-keys).

### create
A Soda SQL CLI command that creates a warehouse directory.

### default metric
An out-of-the-box metric that you can configure in a scan YAML file. By contrast, you can use [SQL metrics]({% link documentation/sql_metrics.md %}#sql-metrics) to define custom metrics in a scan YAML file.

### env_vars YAML
The file in your local user home directory that stores your database login credentials.

### measurement
The value for a metric that Soda SQL obtains during a scan. For example, in `row_count = 5`, `row_count` is the metric and `5` is the measurement.

### metric
A property of the data in your database. See [Metrics]({% link documentation/sql_metrics.md %}). 

### monitor
A scan you define in Soda Cloud that tests the data in your database.

### notification 
A setting you configure in Soda Cloud that defines whom to notify when a data issue triggers an alert. See also: [alert](#alert).

### scan
A Soda SQL CLI command that uses SQL queries to extract information about data in a database table.

### scan YAML
The file in which you configure scan metrics and tests. Soda SQL uses the input from this file to prepare, then run SQL queries against your data. See [Scan YAML]({% link documentation/scan.md %}).

### Soda Cloud
A free, web application that enables you to examine the results of Soda SQL scans and create monitors and alerts. To use Soda Cloud, you must set up and connect Soda SQL to your Soda cloud account.

### Soda SQL
An open-source command-line tool that scans the data in your warehouse. You can use this as a stand-alone tool to monitor data quality from the command-line, or connect it to a Soda Cloud account to monitor your data using a web application.

### SQL metric
A custom metric you define in your scan YAML file. SQL metrics essentially enable you to add SQL queries to your scan YAML file so that Soda SQL runs them during a scan. See [SQL metrics]({% link documentation/sql_metrics.md %}#sql-metrics).

### table
A table in your warehouse.

### table metric
A property of the data in a table in your database. Use a table metric to define tests that apply to all data in the table during a scan. See [Table metrics]({% link documentation/sql_metrics.md %}#table-metrics).

### test
A Python expression that, during a scan, checks metrics to see if they match the parameters defined for a measurement. As a result of a scan, a test either passes or fails. See [Tests]({% link documentation/tests.md %}).

### warehouse
A SQL engine or database that contains data that you wish to test and monitor.

### warehouse directory
The top directory in the Soda SQL directory structure which contains your warehouse YAML file and, generally, your `/tables` directory.

### warehouse YAML
The file in which you configure warehouse connection details and Soda Cloud connection details. See [Warehouse YAML]({% link documentation/warehouse.md %}) and [Connect to Soda Cloud]({% link documentation/connect_to_cloud.md %}).