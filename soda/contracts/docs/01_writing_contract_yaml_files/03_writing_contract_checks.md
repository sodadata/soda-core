# Writing contract checks

* [Schema check](#schema-check)
* [Row count check](#row-count-check)
* [Missing values check](#missing-values-check)
* [Invalid values check](#invalid-values-check)
* [Freshness check](#freshness-check)
* [](#)
* [](#)
* [](#)

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

### Missing values check

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

## Column basic check configurations

On each column, a limited set of basic check types can be configured with some s can be configured with a short style.

```yaml
dataset: CUSTOMERS
columns:
- name: id
  checks:
  - type: no_invalid_values
    valid_format: uuid
  - type: unique
- name: size
  checks:
  - type: no_invalid_values
    valid_values: ['S','M','L']
```

See [more basic column check configuration examples](EXAMPLES.md#basic-column-check-configuration-examples) 

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

# Basic column check configuration examples

# Column check examples

# Common check properties

### Thresholds

TODO explain all the fail_when_  options
TODO explain the how to do in/exclusions in case of ranges
