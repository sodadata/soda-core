In order to make it easier for contract authors to know when they are putting in literal SQL vs Soda Contract interpreted values,
all the keys that are used literally in SQL queries should have `sql` in them.

For example `sql_expression`, `invalid_regex_sql`, `valid_regex_sql` etc
```yaml
dataset: {table_name}
checks:
- type: metric_expression_sql
  metric: us_count
  sql_expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
  must_be: 0
```

Potentially you could consider the column name and data type exceptions to this rule.
Adding `sql` to the keys `name` and `data_type` would be overkill.
```yaml
dataset: {table_name}
columns:
  - name: id
    data_type: VARCHAR
```
