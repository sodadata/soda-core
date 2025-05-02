# Freshness check

### Verify the freshness of the data

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - freshness:
      column: created_at
      threshold:
        must_be_less_than: 2
```

`column` configures a timestamp column in the dataset. (required) 

`unit` configures the unit of measure for the threshold.  Can be `minute`, `hour` or `day` (optional, default is hour) 

`now_variable` The name of the NOW variable. `var.` does not have to be included. (optional, default uses current time from `NOW` in the soda namespace)     
