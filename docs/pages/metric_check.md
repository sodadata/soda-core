# User defined metric check

### Verify a numeric user defined SQL expression

> Note: If you are only applying a numeric aggregate function like eg `avg`, `sum`, 
> consider using [the aggregate check type](aggregate_check.md) instead.  The reason 
> is that aggregate checks provide better support for configuring missing, validity
> and check filters.  Here in metric_expression checks, you have to ensure those aspects 
> are incorporated into your expression.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - metric:
      expression: |
        AVG("end" - "start")
      threshold:
        must_be_between: [9, 11]
```

> The previous example assumes that columns start and end are numeric columns.
> Or more precise, the resulting value of the expression must be a numeric value.
> Expressions that result in other data types like varchars, time periods or 
> timestamps for example are not allowed.

> Metric expression checks can be placed in the dataset checks or in the 
> checks of a column. 

> Note: A threshold is required

The `expression` is a SQL expression that is added to the list of the single 
aggregation query.  This means that the dataset filter is applied to the 
expression of one is configured.

### Verify a numeric user defined SQL query

If you don't need to join other datasets, consider using 
[a user defined metric expression](#verify-a-numeric-user-defined-sql-expression) instead.
Metric expressions are simpler and more performant because they are added to the 
single aggregation query.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - metric:
      query: |
        SELECT AVG("end" - "start")
        FROM "adventureworks"."advw"."dim_employee"
      threshold:
        must_be_between: [9, 11]
```

> Metric checks with a user defined query can be placed in the dataset checks or in the 
> checks of a column.

> Note: A threshold is required

The `query` is a SQL query.
