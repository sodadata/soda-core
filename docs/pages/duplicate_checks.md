# Duplicate checks

### Verify no duplicate values in a single column

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

### Verify no duplicates in a combination of columns

The multi column duplicate check must be placed on the dataset `checks` level 
and must have a `columns` field.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks:
  - duplicate:
      columns: ['country', 'zip']
```

Metric computations: 
* `distinct_count` = count distinct valid values
* `row_count` = row count
* `duplicate_count` = `row_count` - `distinct_count`
* `duplicate_percent` = `duplicate_count` * 100 / `row_count`


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

> Note: This applies to both the single column and the multi column duplicate check.  

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
          threshold:
            metric: percent
            must_be_less_than: 15
```

> Note: This applies to both the single column and the multi column duplicate check.  

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

> Note: This applies to both the single column and the multi column duplicate check.  

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

> Note: This applies **ONLY** to both the single column check.

### Memory footprint note

Both single and multi column duplicate checks are in most data sources do not require a separate query.  That is 
already a big performance gain.  But it's still based on counting the distinct values.  This can be memory intensive 
on large datasets with mostly unique values.
