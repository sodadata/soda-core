---
layout: default
title: Metrics
parent: Documentation
nav_order: 7
---

# Metrics

A **metric** is a property of the data in your database. A **measurement** is the value for a metric that Soda SQL obtains during a scan. For example, in ``row_count = 5``, ``row_count`` is the metric and ``5`` is the measurement.

The following sections detail the configuration for metrics you can customize in your [Scan YAML file]({% link documentation/scan.md %}).



## Table metrics

Use **table metrics** to define tests in your Scan YAML file that apply to all data in the table during a scan.

![table-metrics](../assets/images/table-metrics.png){:height="440px" width="440px"}

Copy+paste:
```shell
tests:
  - row_count > 0
```

| Table metric | Description      | 
| ---------- | ---------------- | 
| ``row_count`` | The total number of rows in a table. | 
| ``schema`` |     |


### Example tests using a table metric

* ``row_count > 0`` checks to see if the table has more than one row. The test passes if the table contains rows.
*  ``row_count = 5`` checks to see if the table has exactly five rows. The test fails if the table contains more or fewer than five rows.

## Column metrics

Use **column metrics** to define tests in your Scan YAML file that apply to specific columns in a table during a scan. 

Where a column metric references a valid or invalid value, or a limit, use the metric in conjunction with a **column configuration**. A Soda SQL scan uses the value of a column configuration key to determine if a test should pass or fail. See [example](#example-tests-using-a-column-metric) below.

![column-metrics](../assets/images/column-metrics.png){:height="440px" width="440px"}

Copy+paste:
```shell
columns:
  id:
    valid_format: uuid
    tests:
      - invalid_percentage == 0
  feepct:
    valid_format: number_percentage
    tests:
      - invalid_percentage == 0
```

| Column metric   | Description |  Use with column config key(s) |
| ---------- | ---------------- | 
| ``missing_count`` | The total number of rows that are missing specific content. | ``missing_values`` <br /> ``missing_regex``|
| ``missing_percentage`` | The total percentage of rows that are missing specific content. | ``missing_values`` <br /> ``missing_regex`` |
| ``values_count`` | The total number of rows that contain content included in a list of valid values. | ``valid_values`` <br /> ``valid_regex``  |
| ``values_percentage`` | The total percentage of rows that contain content included in a list of valid values. | ``valid_values`` <br /> ``valid_regex`` |
| ``valid_count`` |  The total number of rows that contain valid content.  | ``valid_format`` <br /> ``valid_regex``   |
| ``valid_percentage`` | The total percentage of rows that contain valid content.  |  ``valid_format`` <br /> ``valid_regex``  |
| ``invalid_count`` | The total number of rows that contain invalid content.   | ``valid_format`` <br /> ``valid_regex``  |
| ``invalid_percentage`` | The total percentage of rows that contain invalid content.  |  ``valid_format`` <br /> ``valid_regex`` |
| ``min`` | The the smallest value in a numeric column.  |  -  |
| ``max`` | The the greatest value in a numeric column.  |  -  |
| ``avg`` | The average of the values in a numeric coloumn.  |  - |
| ``sum`` | The sum of the values in a numeric column.   | -  |
| ``variance`` | The variance of a numerical column.  | -  |
| ``stddev`` |  The standard deviation of a numeric column.   |   |
| ``min_length`` | The minimum length of a string.  | ``valid_min_length``  |
| ``max_length`` | The maximum length of a string.  | ``valid_max_length``  |
| ``avg_length`` | The average length of a string.  |  -  |
| ``distinct`` |  The distinct contents in rows in a column.  | -  |
| ``unique_count`` | The number of rows in which the content appears only once in the column.  |  - |
| ``duplicate_count`` | The number of rows that contain duplicated content. | -  |
| ``uniqueness`` | A measure of whether the rows contain unique content.  | -  |
| ``maxs`` |  The number of rows that qualify as maximum. | ``valid_max`` |
| ``mins`` |  The number of rows that qualify as minimum. | ``valid_min``  |
| ``frequent_values`` |  The number of rows that contain content that most frequently occurs in the column. |  - |
| ``histogram`` |  A histogram calculated on the content of the column.  | -  |

| Column configuration key  | Description      | 
| ---------- | ---------------- | 
| ``metrics`` | Specifies extra metrics that Soda SQL computes for this column. |
| ``metric_groups`` | Specifies extra metric groups that Soda SQL computes for this column. |
| ``tests`` | A section that contains the tests that Soda SQL runs on a column during a scan.|
| ``missing_values`` | Specifies the values that Soda SQL is to consider missing.|
| ``missing_format`` | Specifies missing values such as whitespace or empty strings.|
| ``missing_regex`` | Use regex expressions to specify your own custom missing values.|
| ``valid_format`` | Specifies valid values with a named valid text format.|
| ``valid_regex`` | Use regex exppressions to specify your own custom valid values. |
| ``valid_values`` | Specifies several valid values in list format. |
| ``valid_min`` | Specifies a min value for valid values. |
| ``valid_max`` | Specifies a max value for valid values. |
| ``valid_min_length`` | Specifies a min length for valid values. |
| ``valid_max_length`` | Specifies a max length for valid values. |


### Example tests using a column metric

* ``invalid_percentage == 0`` in column ``id`` with column configuration ``valid_format: uuid`` checks the rows in the column named ``id`` for values that match a uuid (universally unique identifier) format. If the test passes, it means that 0% of the rows contain data that is invalid; if the test fails, it means that more than 0% of the rows contain invalid data, which is data that is in non-UUID format. 


## Metric groups and dependencies

By default, Soda SQL groups some metrics together because they depend on the same query. When you define a test that uses a metric that is part of a **metric group**, Soda SQL computes all the other metrics in that group during a scan. 

| Metric groups |
| ------------- |
| ``missing_count`` <br /> ``missing_percentage`` <br /> ``values_count`` <br /> ``values_percentage`` |
| ``valid_count`` <br /> ``valid_percentage`` <br /> ``invalid_count`` <br /> ``invalid_percentage`` |
| ``distinct`` <br /> ``unique_count`` <br /> ``uniqueness`` <br /> ``duplicate_count`` |

Further, there exist **dependencies** between some metrics. Where dependencies exist between metrics, the behavior during a scan is the same as with groups: if Soda SQL scans a metric which has dependencies, if includes all the dependencies in the scan as well.

| If you use... | ...the scan includes: |
| ------ | ------------ |
| ``valid_count`` | ``missing_count`` |
| ``valid_percentage`` | ``missing_percentage`` |
| ``invalid_count`` | ``values_count`` |
| ``invalid_percentage``| ``values_percentage``|
| ``missing_count`` <br /> ``missing_percentage`` <br /> ``values_count`` <br /> ``values_percentage`` | ``row_count`` |
| ``histogram`` | ``min`` <br /> ``max`` |



## SQL metrics

Soda SQL comes shipped with the capability to extend our default set of common metrics. This
allows you to compose tests based on custom-made metrics optimized for your dataset.

Custom SQL metrics are specified as a list in your scan YAML file. This property (`sql_metrics`)
can be located as either a root or column-level property.

> If you use a filter, beware that you have to include the filter logic in your SQL metric queries.
> See also section [Variables](#variables) below.

## Basic metric query

The most simple SQL metric is one which selects a single numeric value.
For example:

> By default the name of the field will be used as the name of the metric.  So
> using an alias is the simplest way to specify the metric name.  See
> [Metric names](#metric-names) below if you don't want to specify aliases in your query.


```yaml
table_name: mytable
sql_metrics:
    - sql: |
        SELECT sum(volume) as total_volume_us
        FROM CUSTOMER_TRANSACTIONS
        WHERE country = 'US'
      tests:
        - total_volume_us > 5000
```

## Multiple metrics in one query

You can also compute multiple metric values in a single query. These values can then be combined in your tests:

```yaml
table_name: mytable
sql_metrics:
    - sql: |
        SELECT sum(volume) as total_volume_us,
               min(volume) as min_volume_us,
               max(volume) as max_volume_us
        FROM CUSTOMER_TRANSACTIONS
        WHERE country = 'US'
      tests:
        - total_volume_us > 5000
        - min_volume_us > 20
        - max_volume_us > 100
        - max_volume_us - min_volume_us < 60
```

## Group by queries

It's possible to define group-by sql metrics which allows each test to be checked against
each group combination.  In order for Soda SQL to understand that you're using a
`GROUP BY` it's important to specify the fields you're grouping on. This is done
by setting the `group_fields` property:

```yaml
table_name: mytable
sql_metrics:
    - sql: |
        SELECT country,
               sum(volume) as total_volume,
               min(volume) as min_volume,
               max(volume) as max_volume
        FROM CUSTOMER_TRANSACTIONS
        GROUP BY country
      group_fields:
        - country
      tests:
        - total_volume > 5000
        - min_volume > 20
        - max_volume > 100
        - max_volume - min_volume < 60
```

## Metric names

Defining aliases in your `SELECT` statement is optional. In case you don't want to
do so you'll have to provide the `metric_names` property. This property contains
a list of values which should match the order of values in your `SELECT` statement:

```yaml
table_name: mytable
sql_metrics:
    - sql: |
        SELECT sum(volume),
               min(volume),
               max(volume)
        FROM CUSTOMER_TRANSACTIONS
        WHERE country = 'US'
      metric_names:
        - total_volume_us
        - min_volume_us
        - max_volume_us
      tests:
        - total_volume_us > 5000
        - min_volume_us > 20
        - max_volume_us > 100
        - max_volume_us - min_volume_us < 60
```

## Column SQL metrics

Defining a SQL metric on a column level will also make the column metrics available in
the tests.

```yaml
table_name: mytable
columns:
    metrics:
        - avg
    volume:
        sql_metrics:
            - sql: |
                SELECT sum(volume) as total_volume_us
                FROM CUSTOMER_TRANSACTIONS
                WHERE country = 'US'
              tests:
                - total_volume_us - avg > 5000
```

## Variables

The variables passed in to a scan are also available in the SQL metrics:
Jinja syntax is used to resolve the variables.

> Variables are typically used for [Filtering](filtering.md).  When you use filtering, Soda SQL will not 
> add the filter to your SQL metrics automatic.  You have to include the filter also in your SQL metric queries.
> You are free to use other variables in your SQL metric query.

{% raw %}
```yaml
table_name: mytable
filter: date = DATE '{{ date }}'
sql_metrics:
    - sql: |
        SELECT sum(volume) as total_volume_us
        FROM CUSTOMER_TRANSACTIONS
        WHERE country = 'US' AND date = DATE '{{ date }}'
      tests:
        - total_volume_us > 5000
```
{% endraw %}

## SQL file reference

Instead of inlining the SQL in the YAML files, it's also possible to refer to
a file relative to the scan YAML file:

```yaml
table_name: mytable
sql_metrics:
    - sql_file: mytable_metric_us_volume.sql
      tests:
        - total_volume_us > 5000
```

In this case `mytable_metric_us_volume.sql` contains:
```sql
SELECT sum(volume) as total_volume_us
FROM CUSTOMER_TRANSACTIONS
WHERE country = 'US'
```

