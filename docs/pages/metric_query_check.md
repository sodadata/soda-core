# Metric query check

### Verify a numeric value obtained by a custom SQL query 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - metric_query:
      query: |
        SELECT AVG("end" - "start")
        FROM "adventureworks"."advw"."dim_employee"
      threshold:
        must_be_between: [9, 11]
```

> Expression query checks can be placed in the dataset checks or in the 
> checks of a column.

> Note: A threshold is required

The `query` is a SQL query.
