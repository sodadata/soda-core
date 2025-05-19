# Aggregate check

### Verify the numeric value of an aggregate SQL function 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: age
    checks:
      - aggregate:
          function: avg
          threshold: 
            must_be_between:
              greater_than_or_equal: 5
              less_than_or_equal: 10
````

> Note: A threshold is required

The `function` is case insensitive and must be an aggregate function 
supported by the data source.

### Configure a check filter

Apply the check only to a subset of the data.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: age
    checks:
      - aggregate:
          function: avg
          threshold: 
            must_be_between: 
              greater_than_or_equal: 20
              less_than_or_equal: 50
          filter: |
            "country" = 'USA'
```

### Configure missing and validity

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: age
    checks:
      - aggregate:
          function: avg
          missing_values: [-1]
          invalid_values: [999] 
          threshold: 
            must_be_between:
              greater_than_or_equal: 20
              less_than_or_equal: 50
```
