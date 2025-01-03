# Missing checks

### missing_count check

To verify there are no missing values in a column, add a check 
`type: missing_count` to the column.

```
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
```

The above check will verify that column `id` has no missing values. 

The default threshold requires that `missing_count` must be 0.

### missing_percent check

To verify that the percentage of missing values for a column is not 
allowed to exceed a threshold, use the `missing_percent` check.

The `missing_percent` is the number of missing values relative to the 
total amount of rows.  If there is a filter, it's the amount of rows 
in the filtered dataset.

```
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_percent
```

The default threshold requires that `missing_percent` must be 0.

### Configure a threshold

By default values for the `missing_count` and missing percentage must be 0.

Example: verify that the number of rows with a missing value for column id is 
less than 50.
```
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
        must_be_less_than: 50
```

Example: verify that the percentage of missing values is less then 1%.
```
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_percent
        must_be_less_than: 1
```

For more details on threshold, see [Thresholds](thresholds.md) 

### Configure missing values

By default the `missing_count` and `missing_percent` both count NULL values.  

Extra missing values can be configured for text based and number based columns like this:
```
dataset: dim_employee
columns:

  - name: id
    missing_values: ['No value', 'N/A', '-']
    checks:
      - type: missing_count
  
  - name: age
    missing_values: [-1]
    checks:
      - type: missing_count
```
