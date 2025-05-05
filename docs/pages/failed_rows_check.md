# Failed rows check

### Verify no failed rows

The failed rows check is any query that produces rows that indicate a problem with the data.
In Soda Cloud the failed rows can be sent to people for resolving the problems at the source.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - failed_rows:
      metric: rows_with_high_duration
      query: |
        SELECT *
        FROM "adventureworks"."advw"."dim_employee"
        WHERE ("end" - "start") > 5
```

The `metric` is a name given to the row count value.  You will see this name in 
and the value in the check diagnostics.  The convention is to use lower case 
and underscores

The `query` is a SQL query.
