Verify there are no missing values in the column
```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
```

Verify there are no missing values in the column
and configure a list of value considered as missing in the contract
```yaml
dataset: dim_employee
columns:
  - name: id
    missing_values: ['N/A', '-']
    checks:
      - type: missing_count
```

Verify there are no missing values in the column
and configure a regex matching with values considered missing
```yaml
dataset: dim_employee
columns:
  - name: id
    missing_regex_sql: ^[-]+$
    checks:
      - type: missing_count
```

Verify there are less than 25 missing values in a column:
```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
        must_be_less_than: 25
```

Verify there are between 0 and 1 % missing values in a column:
```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_percent
        must_be_between: [0, 1]
```

Verify specific missing values for a subset of the data
(overriding the missing values for a specific check)
```yaml
dataset: dim_employee
columns:
  - name: id
    missing_values: ['N/A', '-']
    checks:
      - type: missing_count
        missing_values: []
        data_filter_sql: country = 'BE'
        must_be: 0
```
