---
layout: default
title: Tests
parent: Documentation
nav_order: 6
---

# Tests

Tests are evaluated as part of scans and product test results as 
part of the scan result.  When using the CLI, the exit code will be 
determined by the test results.
 
Tests are simple Python expressions where the metrics are available 
as variables. 

For example:

* `min > 25`
* `missing_percentage <= 2.5`
* `5 <= (avg_length / row_count) <= 10`

Tests can be specified on 3 different places:

## Table tests 

Table tests in scan.yml are used for table level metrics like `row_count`.

Available metrics:
* Table metrics like `row_count`
* User defined SQL metrics

Example scan.yml with a table test:
```yaml
table_name: yourtable
metrics:
  - row_count
tests:
  - row_count > 0
```

## Column tests

Column tests in scan.yml are used for testing column level metrics.

Available metrics:
* Table metrics like `row_count`
* User defined SQL metrics
* Column metrics like `missing_count`, `invalid_percentage`, ...

Example scan.yml that specifies a column level:
```yaml
table_name: yourtable
metrics:
    - row_count
    - missing_percentage
    - invalid_percentage
columns:
    start_date:
        - validity_format: date_eu
        - tests:
            - invalid_percentage < 2.0
```

## SQL metric tests 

SQL metric tests in sql metric yaml files are used for testing sql metric values.

Available metrics:
* Table metrics like `row_count`
* User defined SQL metrics

[comment]: <> (TODO if column is specified in sql metric, it should enable the column metrics)
[comment]: <> (* Column metrics like `missing_count`, `invalid_percentage`, ...)

Example SQL metric YAML file that includes SQL metric test:
```yaml
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

## Test names

Tests can be specified in 2 ways:

Anonymous tests are the most simple and easy way to specify tests.

```yaml
...
tests:
    - total_volume_us > 5000
```

Or named.  Named tests are used when sending metrics and test results 
to the Soda cloud platform.  When using named tests, you can update the 
test and still keep the test history linked.  So if you plan to upload your 
tests to the Soda cloud, consider adding unique names.
```yaml
...
tests:
    volume_test_max:  total_volume_us > 3000
    volume_test_min:  total_volume_us < 5000
```