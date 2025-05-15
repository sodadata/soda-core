# Freshness check

### Verify the freshness of the data

Verify there must be values in column `created_at` that are more recent than 2 hours ago: 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - freshness:
      column: created_at
      threshold:
        must_be_less_than: 2
```

`column` configures a timestamp column in the dataset. (required) 

`unit` configures the unit of measure for the threshold.  Can be `minute`, `hour` or `day` 
(optional, default is hour)  See example below.

`now_variable` The name of the NOW variable. `var.` does not have to be included. (optional, 
default uses current time from `NOW` in the soda namespace) See example below.     

### Verify the freshness in days

Verify there must be values in column `created_at` that are more recent than 1 day ago: 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - freshness:
      column: created_at
      threshold:
        unit: day
        must_be_less_than: 1
```

### Specify a now variable name

This enables a user to specify and overwrite the NOW value used in the freshness check via the variables.
It uses the current system time in UTC `${soda.NOW}` as the default.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

variables:
  NOW:
    default: ${soda.NOW}

checks:
  - freshness:
      column: created_at
      now_variable: NOW
      threshold:
        must_be_less_than: 1
```
