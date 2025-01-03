# Row count check

### row_count check

Verify that a dataset has rows with the `row_count` check

For example:
```
dataset: dim_employee

checks:
  - type: row_count
```

### Configure a threshold

Example: verify that the number of rows must be between 10 and 25.
```````
dataset: dim_employee

checks:
  - type: row_count
    must_be_between: [10, 25]
```````

For more details on threshold, see [Thresholds](thresholds.md)
