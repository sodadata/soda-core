# Duplicate checks

### Verify there are no duplicate values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - duplicate:
```

> Note: The default threshold is that `duplicate_count` must be 0

Metric computations: 
* `distinct_count` = count distinct valid values
* `valid_count` = count valid values. This count excludes the missing and invalid values.
* `duplicate_count` = `valid_count` - `distinct_count`
* `duplicate_percent` = `duplicate_count` * 100 / `valid_count`

### Configure a threshold

The `id` column must have less than 5 duplicate values

> Note: Metric duplicate_count adds every duplicate value. Which is different from the 
> number of values that have duplicates. See also 'Metric computations' above. 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - duplicate:
          threshold:
            must_be_less_than: 5
```

### Configure a percentage threshold

The `id` column must have less than 15% of duplicates

> Note: Metric duplicate_count adds every duplicate value. Which is different from the 
> number of values that have duplicates. See also 'Metric computations' above. 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - duplicate:
          metric: percent
          threshold:
            must_be_less_than: 15
```

### Configure a check filter

Apply the check only to a subset of the data.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - duplicate:
          filter: |
            "country" = 'USA'
```

### Configure missing and validity

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - duplicate:
          missing_values: ['-']
          valid_format: 
            regex: ^[A-Z]{3}$
            name: Employee ID
```

### Performance note

The duplicate check is based on counting the distinct values.  This can be 
memory intensive on large datasets with mostly unique values.
