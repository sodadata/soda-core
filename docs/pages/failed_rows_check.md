# Failed rows check

### Verify no failed rows

The failed rows check is any query that produces rows that indicate a problem with the data.
In Soda Cloud the failed rows can be sent to people for resolving the problems at the source.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - failed_rows:
      query: |
        SELECT *
        FROM "adventureworks"."advw"."dim_employee"
        WHERE ("end" - "start") > 5
```

The `query` is a SQL query.
