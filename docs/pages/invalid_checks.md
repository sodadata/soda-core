# Invalid checks

Table of contents
* [Verify there are no invalid values](#verify-there-are-no-invalid-values)
* [Configure invalid values](#configure-invalid-values)
  * [Verify values are in a fixed list of valid values](#verify-values-are-in-a-fixed-list-of-valid-values)
  * [Verify valid values with a regex](#verify-valid-values-with-a-regex) 
  * [Verify valid values occur in a reference dataset](#verify-valid-values-occur-in-a-reference-dataset)
  * [Verify the length of values](#verify-the-length-of-values)
  * [Verify the values are within a range](#verify-the-values-are-within-a-range) 
  * [Verify values are not in a list of invalid values](#verify-values-are-not-in-a-list-of-invalid-values-)
* [Allow some invalid values to occur, up to a threshold](#allow-some-invalid-values-to-occur-up-to-a-threshold)
* [Verify more specific validity on a subset of the data](#verify-more-specific-validity-on-a-subset-of-the-data)
* [Missing values are excluded](#missing-values-are-excluded)

### Verify there are no invalid values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: size
    checks:
      - invalid:
          valid_values: ['S', 'M', 'L']

```

> Note: The default metric is invalid 'count'

> Note: The default threshold is that invalid `count` must be 0.
 
> Note: See [Configure invalid values](#configure-invalid-values) for other ways to configure valid values. 

### Configure invalid values

##### Valid and invalid configurations overview

Multiple configurations can be combined.

| Key                    | Description                                                                                                 | Example link                                                              |
|------------------------|-------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| `valid_values`         | List of values                                                                                              | [Example](#verify-values-are-in-a-fixed-list-of-valid-values)             |
| `valid_format`         | Values not occurring in this list are considered invalid. List of values (strings or numbers).              | [Example](#verify-valid-values-with-a-regex)                              |
| `valid_reference_data` | Values must occur in a reference data column to be valid.                                                   | [Example](#verify-valid-values-occur-in-a-reference-dataset)              |
| `valid_min`            | Values must be greater or equal to the given numeric value to be valid.  Assumes numeric column data type.  | [Example](#verify-the-values-are-within-a-range)                          |
| `valid_max`            | Values must be less or equal to the given numeric value to be valid.  Assumes numeric column data type.     | [Example](#verify-the-values-are-within-a-range)                          |
| `valid_length`         | The length of the column value must be exact as configured. Integer value.                                  | [Example](#verify-the-length-of-values)                                   |
| `valid_min_length`     | The length of the column value must be greater than or equal to the specified numeric value. Integer value. | [Example](#verify-the-length-of-values)                                   |
| `valid_max_length`     | The length of the column value must be less than or equal to the specified numeric value. Integer value.    | [Example](#verify-the-length-of-values)                                   |
| `invalid_values`       | Values occurring in this list are invalid. List of values (strings or numbers)                              | [Example](#verify-values-are-not-in-a-list-of-invalid-values)             |
| `invalid_format`       | A SQL regex that matches with invalid values (Advanced)                                                     | [Example](#verify-values-do-not-match-a-regex-identifying-invalid-values) |

##### Verify values are in a fixed list of valid values

This allows the user to define a fixed list of values to define validity. 

> Limitation: It can be either a list of strings or a list of numbers.  So only ~~~~text and numeric columns are supported for now.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: size
    checks:
      - invalid:
          valid_values: ['S', 'M', 'L']
```

##### Verify valid values with a regex

Configure a regular expression to identify valid values.  It leverages the regex syntax of the SQL engine in the data source.
Every time you use a regex, a human readable name has to be provided as well.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: job_code
    checks:
      - invalid:
          valid_format:
            name: XX_something 
            regex: ^XX[0-9]{4}$
```

See also these [example regexes](example_regexes.md) for inspiration

> Note: The regex expression is passed on as-is in the SQL query so the syntax has to match 
> with the regex syntax used by the data source SQL engine. 

##### Verify valid values occur in a reference dataset

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: country
    checks:
      - invalid:
          valid_reference_data:
            dataset: postgres_adventureworks/adventureworks/advw/country_codes
            column: country_code
```

The `valid_reference_data`.`dataset` is the fully qualified name of the reference dataset.

> Note: `valid_reference_data` is combinable with other missing and validity configurations.

> Performance warning! Using the `valid_reference_data` configuration has a performance impact.  Whereas other 
> validity configurations can be computed in one pass together with all the other metrics, the `valid_reference_data`
> requires its own separate query per check which can make the contract verification run slower and cost more.

##### Verify the length of values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: job_code
    checks:
      - invalid:
          valid_length: 2
```

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: job_code
    checks:
      - invalid:
          valid_min_length: 1
          valid_max_length: 3
```

##### Verify the values are within a range

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: idea_score_pct
    checks:
      - invalid:
          valid_min: 0
          valid_max: 100
```

`valid_min` and `valid_max` can be used individually as well.

##### Verify values are not in a list of invalid values

Configure a list of values that will be considered invalid.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: desk
    checks:
      - invalid:
          invalid_values: ['Cocobola', 'Rubber']
```

##### Verify values do not match a regex identifying invalid values

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: desk
    checks:
      - invalid:
          invalid_format:
            name: a1z or a6z 
            regex: ^a[16]z$
```

See also these [example regexes](example_regexes.md) for inspiration

### Allow some invalid values to occur, up to a threshold

Verify there are less than 25 invalid values in a column:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: size
    checks:
      - invalid:
          valid_values: ['S', 'M', 'L']
          threshold:
            must_be_less_than: 25
```

Verify there are between 0 and 1 % missing values in a column:
The default metric is missing `count`.  Specify `metric: percent` to 
set the threshold as a percent of the total row count.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - type: invalid
        threshold:
          metric: percent
          must_be_between: 
            greater_than_or_equal: 0
            less_than_or_equal: 1
```

The metric used in this check type is `invalid_percent`, which is calculated 
as: `invalid_count` x `100` / `row_count`

> Note: In case there are no rows, the invalid `percent` is considered 0 to 
> avoid a division by zero.     

For more details on threshold, see [Thresholds](thresholds.md) 

### Missing values are excluded

All the missing values configurations mentioned in [missing configurations overview](missing_checks.md#missing-configurations-overview)
can also be specified here in the invalid check type.  Missing values will be excluded from the invalid values.  

When customizing the missing values beyond NULL, note that these custom missint values are also excluded from the validity checks.
This ensures that missing values are not counted double: `missing_count` + `invalid_count` + `valid_count` must be equal to `row_count`

Same reasoning: `missing_percent` + `invalid_percent` + `valid_percent` = 100
