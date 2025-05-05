# Metric expression check

> Note: If you are only applying a numeric aggregate function like eg `avg`, `sum`, 
> consider using [the aggregate check type](aggregate_check.md) instead.  The reason 
> is that aggregate checks provide better support for configuring missing, validity
> and check filters.  Here in metric_expression checks, you have to ensure those aspects 
> are incorporated into your expression.

### Verify the numeric value of a custom SQL expression 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - metric_expression:
      metric: avg_duration
      expression: |
        AVG("end" - "start")
      threshold:
        must_be_between: [9, 11]
```

> Metric expression checks can be placed in the dataset checks or in the 
> checks of a column.

> Note: A threshold is required

The `metric` is a name given to the expression value.  You will see this name in 
and the value in the check diagnostics.  The convention is to use lower case 
and underscores

The `expression` is a SQL expression that is added to the list of the single 
aggregation query.  This means that the dataset filter is applied to the 
expression of one is configured.
