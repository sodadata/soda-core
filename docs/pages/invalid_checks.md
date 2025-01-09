# Invalid checks

### invalid_count check

To verify there are no invalid values in a column, add a check 
`type: invalid_count` to the column.

An invalid check requires a validity configuration like for example `valid_values`

```
dataset: dim_employee
columns:
  - name: category
    valid_values: ['A', 'B', 'C']
    checks:
      - type: invalid_count
```

The above check will verify that column `category` has no invalid values
because the default threshold requires that `invalid_count` must be 0.

TODO continue
