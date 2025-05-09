# Failed rows check

The failed rows check is based on user defined SQL that produces rows that indicate a 
problem with the data. The rows returned should also include diagnostic information.

In Soda Cloud the failed rows can be viewed and assigned to people for resolving the 
problems at the source.

### Verify no failed rows with a user defined SQL expression

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - failed_rows:
      expression: |
        ("end" - "start") > 5
```

The `query` is a user defined SQL expression.

### Verify no failed rows with a user defined SQL query

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
