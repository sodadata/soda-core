# Writing contract checks

* [Schema check](#schema-check)
* [Row count check](#row-count-check)
* [Missing values check](#missing-values-checks)
* [Invalid values check](#invalid-values-checks)
* [Uniqueness check](#uniqueness-check)
* [Freshness check](#freshness-check)
* [Avg, sum and other SQL aggregation checks](#avg-sum-and-other-sql-aggregation-checks)
* [Multi-column uniqueness check](#multi-column-uniqueness-check)
* [User-defined SQL check](#user-defined-sql-check)

### Schema check

When verifying a contract, Soda will first check the schema.  The schema is essential and will always be checked.

Example contract schema check:
```yaml
dataset: CUSTOMERS
columns:
- name: id
  data_type: VARCHAR
- name: size
- name: discount
  optional: true
```

Optionally, if the `data_type` property is specified in the column, the data type will be checked as well as part of
the schema check.

By default all columns specified in the contract are required. Columns can also be marked as `optional: true`.

See the examples below for more schema features like optional columns and allowing other columns.

### Row count check

Example: Simplest row count check
```yaml
dataset: CUSTOMERS
columns:
  - ...
checks:
  # Check if the row count is greater than 0
  - type: row_count
```

Example: Row count check with a range
```yaml
dataset: CUSTOMERS
columns:
  - ...
checks:
  # Check if the row count is not between 100 and 120
  - type: row_count
    fail_when_not_between: [100, 120]
```

See [Thresholds](#thresholds) for more on specifying failure thresholds and ranges.

### Missing values checks

Example: simplest not-null check. By default the missing values applies the `fail_when_greater_than: 0` threshold.
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are NULL values in CUSTOMERS.id
  - type: missing_count
```

Example: configure optional threshold
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are more than 10 NULL values in CUSTOMERS.id
  - type: missing_count
    fail_when_greater_than: 10
```

Example: configure optional missing_values list
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are missing values in CUSTOMERS.id where `'N/A'` and `'No value'` are considered missing values.
  - type: missing_count
    missing_values: ['N/A', 'No value']
```

Example: configure optional missing_regex
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are missing values in CUSTOMERS.id where missing values are specified with a SQL regex
  - type: missing_count
    missing_regex: ^(NULL|null)$
```

Missing values also can have the [common check properties](#common-check-properties)

### Invalid values checks

Example: Validate against a list of valid values in the contract

```yaml
dataset: CUSTOMERS
columns:
- name: size
  checks:
  # Fail when there are values not in the given list of valid values
  - type: invalid_count
    valid_values: ['S', 'M', 'L']
```

Example of valid min-max checks
```yaml
dataset: CUSTOMERS
columns:
- name: market_share_pct
  checks:
  # Fail when there are values not between the min and max value
  - type: invalid_count
    valid_min: 0
    valid_max: 100
```


Example of valid length checks as a range
```yaml
dataset: CUSTOMERS
columns:
- name: comment
  checks:
  # Fail when there are values not between the min and max length
  - type: invalid_count
    valid_min_length: 1
    valid_max_length: 144
```

Example of a fixed valid length check
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are values not a fixed length of 5
  - type: invalid_count
    valid_length: 5
```

Example of a valid SQL regex check
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are values not matching a SQL regex
  - type: invalid_count
    valid_regex: '^ID.$'
```

Example of a reference check, (aka referential integrity, foreign key)
```yaml
dataset: CUSTOMERS
columns:
- name: category_id
  checks:
  # Fail when there are values not occuring in another column of another dataset
  - type: invalid_count
    valid_values_column:
        dataset: CUSTOMER_CATEGORIES
        column: id
```

Example of combing missing & invalid:
```yaml
dataset: CUSTOMERS
columns:
- name: size
  checks:
  # In case there are missing value customizations (apart from NULL, which is always missing)...
  - type: missing
    missing_values: ['N/A']
  # The invalid values check will ignore the missing values.  This is to ensure that
  # missing_count + invalid_count + valid_count = row_count
  - type: invalid_count
    valid_values: ['S', 'M', 'L']
```
Caveats:
* Ensure that the missing check and missing configuration is declared * before * the invalid check
* In the (unlikely) case that there are multiple missing checks with missing values configs, they are overriding (not merging). Last one wins.
* This ignoring of missing values probably doesn't work when using valid_values_column configuration

### Uniqueness check

Example of the simplest uniqueness check
```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  # Fail when there are duplicates
  - type: duplicate_count
```

### Freshness check

Checks if there are rows indicating recent data has been added.  It assumes there is a column that represents a timestamp like
an event time or so.  The check looks for the maximum value in the column and verifies if that maximum value is not older than
a given threshold time period.

Example
```yaml
dataset: CUSTOMERS
checks:
    - type: freshness_in_hours
      fail_when_greater_than: 6
```

| All freshness check types    |
|------------------------------|
| `type: freshness_in_days`    |
| `type: freshness_in_hours`   |
| `type: freshness_in_minutes` |

### Avg, sum and other SQL aggregation checks

Exmple of an average check
```yaml
dataset: CUSTOMERS
columns:
- name: size
  checks:
  # Fail when the average is not between 10 and 20
  - type: avg
    fail_when_not_between: [10, 20]
```

| Numeric SQL aggregation check types |
|-------------------------------------|
| `type: avg`                         |
| `type: sum`                         |
| `type: min`                         |
| `type: max`                         |
| `type: stddev`                      |
| `type: stddev_pop`                  |
| `type: stddev_samp`                 |
| `type: variance`                    |
| `type: var_pop`                     |
| `type: var_samp`                    |

### Multi-column uniqueness check

Example of a multi columns duplicates check

```yaml
dataset: CUSTOMERS
columns:
- ...
checks:
- type: multi_column_duplicates
  columns: ['country_code', 'zip']
```

### User defined SQL checks

Example of a user-defined SQL expression check

```yaml
dataset: CUSTOMERS
columns:
- name: country
  checks:
  - type: sql_expression
    metric: us_count
    metric_sql_expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
    fail_when_not_between: [100, 120]
```

Example of a user-defined SQL query check

```yaml
dataset: CUSTOMERS
columns:
- ...
checks:
- type: user_defined_sql
  metric: us_count
  query: |
    SELECT COUNT(*)
    FROM {table_name}
    WHERE country = 'US'
  fail_when_between: [0, 5]
```

### Filter

Sometimes new data is already appended in an incremental table and you only want to run the 
contract checks on the new data.  For this, a SQL filter can be used.

The following check requires a variable to be passed.

```yaml
dataset: CUSTOMERS
sql_filter: |
  created > ${FILTER_START_TIME}
columns:
  - name: id
  - name: created
checks:
  - type: row_count
    fail_when_less_than: 10
```

### Thresholds

Some check types have default thresholds.  If you do want to specify a threshold, use one of these check configuration properties

| Threshold key                     | Example                                |
|-----------------------------------|----------------------------------------|
| `fail_when_is`                    | `fail_when_is: 0`                      |
| `fail_when_is_not`                | `fail_when_is_not: 0`                  |
| `fail_when_greater_than`          | `fail_when_greater_than: 100`          |
| `fail_when_greater_than_or_equal` | `fail_when_greater_than_or_equal: 100` |
| `fail_when_less_than`             | `fail_when_less_than: 100`             |
| `fail_when_less_than_or_equal`    | `fail_when_less_than_or_equal: 100`    |
| `fail_when_between`               | `fail_when_between: [0, 100]`          |
| `fail_when_not_between`           | `fail_when_not_between: [0, 100]`      |

TODO explain the how to do in/exclusions in case of ranges
