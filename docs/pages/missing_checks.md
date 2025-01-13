# Missing checks

Table of contents
* [Verify there are no missing values](#verify-there-are-no-missing-values)
* [Configure values that are considered as missing](#configure-values-that-are-considered-as-missing)
* [Allow some missing values to occur, up to a threshold](#allow-some-missing-values-to-occur-up-to-a-threshold)

### Verify there are no missing values

Verify there are no missing values in the column: 

```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
```

> The default threshold requires that `missing_count` must be 0.

### Configure values that are considered as missing

NULL is always considered a missing value.  Only configure the extra 
non-NULL values that must be considered as missing. Typical examples are 
'-', 'No value', 'N/A', 'None', 'null', -1, 999

##### Specify a list of extra values that apart from NULL, are also considered as missing values:

```yaml
dataset: dim_employee
columns:
  - name: id
    missing_values: ['N/A', '-']
    checks:
      - type: missing_count
```

`missing_values` is a list of values.  All strings or all numeric values are supported.

> Note `missing_values` is on the column level. That's because of 2 reasons:
> * Multiple checks will use the missing values configuration.  This way you don't have to 
>   repeat the missing values configuration and maintain the duplicate configurations. 
> * It separates the metadata (information describing the column) from the actual check. 

##### Specify a regex that apart from NULL, matches with values that are also considered as missing:

```yaml
dataset: dim_employee
columns:
  - name: id
    missing_regex_sql: ^[-]+$
    checks:
      - type: missing_count
```

`missing_regex_sql` is interpreted by the data source warehouse SQL engine.

For the full list of options, see [the Reference list of missing value configuration keys below](#list-of-missing-value-configuration-keys) 

### Allow some missing values to occur, up to a threshold

Verify there are less than 25 missing values in a column:

```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
        must_be_less_than: 25
```

Verify there are between 0 and 1 % missing values in a column:

```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_percent
        must_be_between: [0, 1]
```

The metric used in this check type is `missing_percent`, which is calculated 
as: `missing_count` x `100` / `row_count`

> Pro tip: It can be more informative to use missing_percent as the diagnostic 
> information for this check will include the actual row_count, missing_count and 
> missing_percentage values.

> Warning: A `missing_percent` check can only be evaluated if there are rows.
> That's because division by zero is not possible.  A `missing_percent` check 
> will have an outcome value of UNEVALUATED in case there are no rows. This 
> will cause the contract verification to fail while there are no rows with 
> missing values.   

For more details on threshold, see [Thresholds](thresholds.md) 

### List of missing value configuration keys

There are missing value configuration keys 

| Key                 | Description                                           | Examples                               |
|---------------------|-------------------------------------------------------|----------------------------------------|
| `missing_values`    | A list of values that represent missing data          | ['N/A', '-', 'No value']<br/>[-1, 999] |
| `missing_regex_sql` | A warehouse SQL regex that matches for missing values | ^(-)+$                                 |

### List of checks supporting the missing configuration keys

These check types support the missing configuration keys:

* `missing_count`
* `missing_percent`
* `invalid_count`
* `invalid_percent`
* `nok_count`
* `nok_percent`
