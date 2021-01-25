---
layout: default
title: Scan
parent: Documentation
nav_order: 5
---

# Scan

This section explains what a scan does, how it works and how to use `scan.yml` files to configure them.

For running scans, see either [the CLI]({% link documentation/cli.md %}) or [Orchestrate scans]({% link documentation/orchestrate_scans.md %}).

## Anatomy of a scan

A scan is performed on a table and does the following:

* Fetch the column metadata of the table (column name, type and nullable)
* Single aggregation query that computes aggregate metrics for multiple columns like e.g. missing, min, max etc
* For each column
  * One query for distinct_count, unique_count and valid_count
  * One query for mins (list of smallest values)
  * One query for maxs (list of greatest values)
  * One query for frequent values
  * One query for histograms

> Note on performance: we have tuned most column queries by using the same Column Table Expression (CTE).
The goal is to allow some databases, like eg Snowflake, to be able to cache the results, but we didn't see
actual proof of this yet.  If you have knowledge on this, [drop us a line in one of the channels]({% link community.md %}).

## Top level scan.yml keys

In a `scan.yml` file, you configure which metrics should be computed and
which tests should be checked.

Top level configuration keys:

| Key | Description | Required |
| --- | ----------- | -------- |
| table_name | The table name. | Required |
| metrics | The list of metrics to compute. Column metrics specified here will be computed on each column. | Optional |
| columns | Optionally add metrics and configurations for specific columns | Optional |
| filter | A SQL expression that will be added to query where clause. Uses [Jinja as template language](https://jinja.palletsprojects.com/). Variables can be passed into the scan.  See [Filtering]({% link documentation/filtering.md %}) | Optional |
| mins_maxs_limit | Max number of elements for the mins metric | Optional, default is 5 |
| frequent_values_limit | Max number of elements for the maxs metric | Optional, default is 5 |
| sample_percentage | Adds [sampling](https://docs.snowflake.com/en/sql-reference/constructs/sample.html) to limit the number of rows scanned. Only tested on Postgres | Optional |
| sample_method | For Snowflake, One of { BERNOULLI, ROW, SYSTEM, BLOCK } | Required if sample_percentage is specified |

## Metrics

### Table metrics

| Meric | Description |
| ----- | ------------|
| row_count |  |
| schema |  |

### Column metrics

| Meric | Description |
| ----- | ------------|
| missing_count |  |
| missing_percentage |  |
| values_count |  |
| values_percentage |  |
| valid_count |  |
| valid_percentage |  |
| invalid_count |  |
| invalid_percentage |  |
| min |  |
| max |  |
| avg |  |
| sum |  |
| variance |  |
| stddev |  |
| min_length |  |
| max_length |  |
| avg_length |  |
| distinct |  |
| unique_count |  |
| duplicate_count |  |
| uniqueness |  |
| maxs |  |
| mins |  |
| frequent_values |  |
| histogram |  |

### Metric categories

> Deprecated metric categories are now included in the metrics, but that is probably not a
> good idea. We're considering to introduce `metric_categories` as a separate top level element

| Meric category | Metrics |
| -------------- | ------------|
| missing | missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage |
| validity | valid_count<br/>valid_percentage<br/>invalid_count<br/>invalid_percentage |
| duplicates | distinct<br/>unique_count<br/>uniqueness<br/>duplicate_count |

### Implied metrics

| Any metric in | Implies metrics |
| ------------- | --------------- |
| valid_count<br/>valid_percentage<br/>invalid_count<br/>invalid_percentage | missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage |
| missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage | row_count |
| histogram | min<br/>max |

## Column configurations

Column configuration keys:

| Key | Description |
| --- | ----------- |
| metrics | Extra metrics to be computed for this column |
| tests | Tests to be evaluate for this column |
| missing_values | Customize what values are considered missing |
| missing_format | To customize missing values such as whitespace and empty strings |
| missing_regex | Define your own custom missing values |
| valid_format | Specifies valid values with a named valid text format |
| valid_regex | Specifies valid values with a regex |
| valid_values | Specifies valid values with a list of values |
| valid_min | Specifies a min value for valid values |
| valid_max | Specifies a max value for valid values |
| valid_min_length | Specifies a min length for valid values |
| valid_max_length | Specifies a max length for valid values |