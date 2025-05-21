# Missing checks

Table of contents
* [Verify there are no missing values](#verify-there-are-no-missing-values)
* [Configure values that are considered as missing](#configure-values-that-are-considered-as-missing)
* [Allow some missing values to occur, up to a threshold](#allow-some-missing-values-to-occur-up-to-a-threshold)

### Verify there are no missing values

Verify there are no missing values in the column: 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
```

> The default metric used is `count`

> The default threshold requires that missing `count` must be 0.

> The metrics missing `count`, missing `percent` and `row_count` will be added 
> as diagnostic metrics for each evaluation. 

### Configure a different missing metric

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          threshold:
            metric: percent
```

### Configure extra missing values

NULL is always considered a missing value.  Only configure the extra 
non-NULL values that must be considered as missing. Typical examples are 
'-', 'No value', 'N/A', 'None', 'null', -1, 999

##### Missing configurations overview

| Key              | Description                                                                                                                            | Example link                                                         |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| `missing_values` | List of values that apart from NULL is considered missing.  NULL is always considered a missing value and doesn't need to be included  | [Example](#configure-extra-missing-values)                           | 
| `missing_format` | A SQL regex that matches with missing values. (Advanced)                                                                               | [Example](#configure-a-regular-expression-to-specify-missing-values) |

##### Configure a list of extra missing values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          missing_values: ['N/A', '-']
```

`missing_values` is a list of values.  All strings or all numeric values are supported.

> Note for now, we only recommend `missing_values` is on the check level. We may consider 
> to configure missing and validity configurations on the column level because 
> of 2 reasons:
> * Multiple checks will use the missing values configuration.  This way you don't have to 
>   repeat the missing values configuration and maintain the duplicate configurations. 
> * It separates the metadata (information describing the column) from the actual check.
> The engine already supports the configurations on the column level, but the editor not yet. 
> All feedback welcome.

### Configure a regular expression to specify missing values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          missing_format:
            name: All dashes
            regex: ^[-]+$
```

`missing_format.regex` is interpreted by the data source warehouse SQL engine.

For the full list of options, see [the Reference list of missing value configuration keys below](#list-of-missing-value-configuration-keys) 

### Allow some missing values to occur, up to a threshold

Verify there are less than 25 missing values in a column:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          threshold:
            must_be_less_than: 25
```

Verify there are between 0 and 1 % missing values in a column:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          threshold:
            metric: percent
            must_be_between:
              greater_than_or_equal: 0
              less_than_or_equal: 1
```

The metric used in this check type is missing `percent`, which is calculated 
as: `missing_count` x `100` / `row_count`

> Note: If there are no rows, to check, a missing `percent` check avoids the 
> division by zero and concludes there are 0 % missing values.  

For more details on threshold, see [Thresholds](thresholds.md) 
