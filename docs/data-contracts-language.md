# Data contract check reference

Soda data contracts is a Python library that verifies data quality standards as early and often as possible in a data pipeline so as to prevent negative downstream impact. Be aware, Soda data contracts checks do not use SodaCL. Learn more [About Soda data contracts](https://docs.soda.io/soda/data-contracts.html).

> [!IMPORTANT]
> Experimentally supported in Soda Core 3.3.3 or greater for PostgreSQL, Snowflake, and Spark

What follows is reference documentation and examples of each type of data contract check. Note that data contracts checks do not follow SodaCL syntax. 

[Duplicate](#duplicate)<br />
[Freshness](#freshness)<br />
[Missing](#missing)<br />
[Row count](#row-count)<br />
[SQL aggregation](#sql-aggregation)<br />
[SQL metric expression](#sql-metric-expression)<br />
[SQL metric query](#sql-metric-query)<br />
[Validity](#validity)<br />
[Check filters](#check-filters)<br >
[List of threshold keys](#list-of-threshold-keys)<br />
<br />

## Duplicate

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `no_duplicate_values` | no  | - | `name`<br /> `columns`  |
| `duplicate_count`     | required | - | `name`  |
| `duplicate_percent`   | required | - | `name`  |


```yaml
dataset: dim_employee

...

columns:
- name: id
  checks:
  - type: no_duplicate_values
- name: last_name
  checks:
  - type: duplicate_count
    must_be_less_than: 10
    name: Fewer than 10 duplicate names
- name: address_line1
  checks:
  - type: duplicate_percent
    must_be_less_than: 1

checks:
- type: no_duplicate_values
  columns: ['phone', 'email']
```

## Freshness
This check compares the maximum value in the column to the time the scan runs; the check fails if that computed value exceeds the threshold you specified in the check.

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `freshness_in_days`   | required  | - | `name`  |
| `freshness_in_hours`  | required  | - | `name`  |
| `freshness_in_minutes`| required  | - | `name`  |


```yaml
dataset: dim_customer

...

columns:
- name: date_first_purchase
  checks:
    type: freshness_in_days
    must_be_less_than: 2
    name: New data arrived within the last 2 days
```

## Missing
If you *do not* use an optional column configuration key to identify the values Soda ought to consider as missing, Soda uses NULL to identify missing values. 

See also: [Combine missing and validity](#combine-missing-and-validity)

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | ------------------------------- | --------------------------------- |
| `no_missing_values` | no  | - | `name`<br /> `missing_values`<br /> `missing_sql_regex`<br />  |
| `missing_count`     | required | - | `name`<br /> `missing_values`<br /> `missing_sql_ regex`<br />  |
| `missing_percent`   | required | - | `name`<br /> `missing_values`<br /> `missing_sql_regex`<br />  |


```yaml
dataset: dim_customer

...

columns: 
- name: title
  checks: 
  - type: no_missing_values 
- name: middle_name
  checks: 
  - type: missing_count
    must_be_less_than: 10
    # Soda includes 'NULL' in list of values by default
    missing_values: ['xxx', 'none', 'NA']
- name: last_name
  checks:
  - type: missing_count
    must_be_less_than: 5 
- name: first_name
  checks: 
  - type: missing_percent
    must_be_less_than: 1
    name: No whitespace entries
    # regular expression must match the dialect of your SQL engine
    missing_sql_regex: '[\s]'
```


## Row count

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys) | Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `rows_exist`    | no  | - | `name`  |
| `row_count`     | required | - | `name`  |


```yaml
dataset: dim_customer

...

columns: 
- name: first_name
  checks: 
  - type: row_count
    must_be_between: [100, 120]
    name: Verify row count range

checks: 
- type: rows_exist
```

## SQL aggregation

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `avg`   | required  | - | `name`  |
| `sum`  | required  | - | `name`  |


```yaml
dataset: dim_customer

...

columns:
- name: yearly_income
  checks:
  - type: avg
    must_be_between: [50000, 80000]
    name: Average salary within expected range

- name: total_children
  checks:
  - type: sum
    must_be_less_than: 10
```

<!--## SQL failed rows

TODO: It's on the roadmap to support capturing of failed rows in contracts.-->

## SQL metric expression

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `metric_expression`   | required  | - | `name`  |

Use a SQL metric expression check to monitor a custom metric that you define using a SQL expression. 

You can apply a SQL metric check to one or more columns or to an entire dataset. As Soda data contracts pushes results to Soda Cloud, it associates column checks with the column name in Soda Cloud.

Relative to a [SQL metric query](#sql-metric-query) check, a SQL metric expression check offers slightly better performance during contract verification. In the case where Soda must also compute other metrics during verification, it appends a SQL metric expression to the same query so that it only requires a single pass over the data to compute all the metrics. A SQL metric query executes independently of other queries during verification, essentially requiring a separate pass.


```yaml
dataset: CUSTOMERS
...

columns:
  - name: id
  # SQL metric expression check for a column
  - name: country
    checks:
    - type: metric_expression
      metric: us_count
      expression_sql: COUNT(CASE WHEN country = 'US' THEN 1 END)
      must_be: 0
```

<br />

```yaml
dataset: CUSTOMERS
...

columns:
  - name: id
  - name: country
checks:
# SQL metric expression check for a dataset
- type: metric_expression
  metric: us_count
  expression_sql: COUNT(CASE WHEN country = 'US' THEN 1 END)
  must_be: 0
```

<br />

## SQL metric query

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | :-------------------------------: | --------------------------------- |
| `metric_expression`   | required  | - | `name`  |

Use a SQL metric query check to monitor a custom metric that you define using a SQL query.

You can apply a SQL metric check to one or more columns or to an entire dataset. As Soda data contracts pushes results to Soda Cloud, it associates column checks with the column name in Soda Cloud.


```yaml
dataset: CUSTOMERS
...

columns:
  # SQL metric query check for a column
  - name: id
    checks:
    - type: metric_query
      metric: us_count
      query_sql: |
        SELECT COUNT(*)
        FROM {table_name}
        WHERE country = 'US'
      must_be_not_between: [0, 5]
  - name: country
```

<br />

```yaml
dataset: CUSTOMERS
...

columns:
  - name: id
checks:
  # SQL metric expression check for a dataset
  - type: metric_query
    metric: us_count
    query_sql: |
      SELECT COUNT(*)
      FROM {table_name}
      WHERE country = 'US'
    must_be_not_between: [0, 5]
```


## Validity

| Type of check | Accepts <br /> [threshold values](#list-of-threshold-keys)| Column config <br />keys: required | Column config <br />keys: optional |
|---------------- | :-------------: | --------------------------------- | --------------------------------- |
| `no_invalid_values` | no  | At least one of:<br /> `valid_values`<br /> `valid_format` [Valid formats](#valid-formats)<br /> `valid_sql_regex`<br /> `valid_min`<br /> `valid_max`<br /> `valid_length`<br /> `valid_min_length`<br /> `valid_max_length`<br /> `valid_values_reference_data`<br /> `invalid_values`<br /> `invalid_format`<br /> `invalid_sql_regex`| `name`  |
| `invalid_count`     | required | At least one of:<br />`valid_values`<br /> `valid_format` [Valid formats](#valid-formats)<br /> `valid_sql_regex`<br /> `valid_min`<br /> `valid_max`<br /> `valid_length`<br /> `valid_min_length`<br /> `valid_max_length`<br /> `valid_values_reference_data`<br /> `invalid_values`<br /> `invalid_format`<br /> `invalid_sql_regex` | `name`  |
| `invalid_percent`   | required | At least one of:<br />`valid_values`<br /> `valid_format` [Valid formats](#valid-formats)<br /> `valid_sql_regex`<br /> `valid_min`<br /> `valid_max`<br /> `valid_length`<br /> `valid_min_length`<br /> `valid_max_length`<br /> `valid_values_reference_data`<br /> `invalid_values`<br /> `invalid_format`<br /> `invalid_sql_regex` | `name`  |


```yaml
dataset: dim_customer

...

columns: 
- name: first_name
  data_type: character varying
  checks: 
  - type: no_invalid_values
    valid_min_length: 2
- name: email_address
  checks: 
  - type: invalid_count
    must_be_less_than: 25
    valid_format: email
- name: id
  checks:
  - type: invalid_percent
    must_be_less_than: 5
    valid_sql_regex: '^ID.$'
    name: Less than 5% invalid
- name: total_children
  checks:
  - type: invalid_count
    # With multiple configurations, rows must meet ALL criteria
    valid_min: 0
    valid_max: 12
    must_be_less_than: 10
    name: Acceptable range of offspring count
  - name: comment
    checks:
    - type: no_invalid_values
      valid_min_length: 0
      valid_max_length: 160
```

<br />

### Valid formats

For a list of the available formats to use with the `valid_formats` column configuration key, see: [List of valid formats](https://docs.soda.io/soda-cl/validity-metrics.html#list-of-valid-formats) for SodaCL.

<br />

### Validity reference

Also known as a referential integrity or foreign key check, Soda executes a validity check with a `valid_values_reference_data` column configuration key as a separate query, relative to other validity queries. The query counts all values that exist in the named column which also *do not* exist in the column in the referenced dataset. 

The referential dataset must exist in the same data source as the dataset identified by the contract.


```yaml
dataset: dim_employee

...

columns:
- name: country
  checks:
  - type: invalid_percent
    must_be_less_than: 3
    valid_values_reference_data: 
      dataset: countryID
      column: id
```

<br />

### Combine missing and validity

You can combine column configuration keys to include both missing and validity parameters. Soda separately evaluates the parameters to prevent double-counting any rows that fail to meet the specified thresholds so that a row that fails both parameters only counts as one failed row.

```yaml
dataset: dim_product

...

columns:
- name: size
  checks:
  - type: no_invalid_values
    missing_values: ['N/A']
    valid_values: ['S', 'M', 'L']
```

<br />

If you add both a missing and validity check to a single column, Soda leverages the results of preceding checks when evaluating subsequent ones. 

In the example below, Soda considers any row that failed the `no_missing_values` check as one that will fail the second, `no_invalid_values` check without re-evaluating the row. 

```yaml
dataset: dim_product

...

columns:
- name: size
  checks:
  - type: no_missing_values
    missing_values: ['N/A']
  - type: no_invalid_values
    valid_values: ['S', 'M', 'L']
```

In the case where you have configured multiple missing checks that specify different missing values, Soda does not merge the results of the check evaluation; it only honors that last set of missing values. Not supported by `valid_values_reference_data`.

<br />

## Check filters

Optionally, you can apply a `filter_sql` for the following checks:

* numeric metrics, except `duplicate_count` and `duplicate_percent`
* `no_missing_values`, `missing_count` and `missing_percent`
* `no_invalid_values`, `invalid_count` and `invalid_percent`

The example below verifies that the only valid value for the column `currency` is `pounds` when the value of the `country` column for the fow is `UK`. 

```yaml
dataset: dim_product

...

columns:
- name: country
- name: currency
  checks:
  - type: no_invalid_values
    valid_values: ['pounds']
    filter_sql: country = 'UK'
```

## List of threshold keys

| Threshold key | Expected value | Example |
| -------------- | ------------- | ------- |
| `must_be`                          | number            | `must_be: 0`                            |
| `must_not_be`                      | number            | `must_not_be: 0`                        |
| `must_be_greater_than`             | number            | `must_be_greater_than: 100`             |
| `must_be_greater_than_or_equal_to` | number            | `must_be_greater_than_or_equal_to: 100` |
| `must_be_less_than`                | number            | `must_be_less_than: 100`                |
| `must_be_less_than_or_equal_to`    | number            | `must_be_less_than_or_equal_to: 100`    |
| `must_be_between`                  | list of 2 numbers | `must_be_between: [0, 100]`             |
| `must_be_not_between`              | list of 2 numbers | `must_be_not_between: [0, 100]`         |


#### Threshold boundaries

When you use `must_be_between` threshold keys, Soda includes the boundary values as acceptable. In the following example, a check result of `100` or `120` each passes.

```yaml
dataset: dim_customer

columns:
- name: first_name
- name: middle_name
- name: last_name

checks:
- type: row_count
  must_be_between: [100, 120]
```
<br />

When you use `must_be_between` threshold keys, Soda includes the boundary values as acceptable. In the following example, a check result of `0` or `120` each fails.

```yaml
dataset: dim_customer

columns:
- name: first_name
- name: middle_name
- name: last_name

checks:
- type: row_count
  must_be_not_between: [0, 120]
```

<br />

Use multiple thresholds to adjust the inclusion of boundary values.

```yaml
dataset: dim_customer

columns:
- name: total_children
  # check passes if values are outside the range, inclusive of 20 
  checks:
  - type: avg
    must_be_less_than: 10
    must_be_greater_than_or_equal_to: 20
- name: yearly_income
  # check passes if values are inside the range, inclusive of 100
  checks:
  - type: avg
    must_be_greater_than_or_equal_to: 100
    must_be_less_than: 200
```


## Help
Need help? Join the <a href="https://community.soda.io/slack" target="_blank"> Soda community on Slack</a>.


