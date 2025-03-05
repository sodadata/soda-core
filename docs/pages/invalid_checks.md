# Invalid checks

Table of contents
* [Verify there are no invalid values](#verify-there-are-no-invalid-values)
* [Configure invalid values](#configure-invalid-values)
  * [Verify values are in a fixed list of valid values](#verify-values-are-in-a-fixed-list-of-valid-values)
  * [Verify values are in a named format](#verify-values-are-in-a-named-format)
  * [Verify valid values with a regex](#verify-valid-values-with-a-regex-) 
  * [Verify valid values occur in a reference dataset](#verify-valid-values-occur-in-a-reference-dataset)
  * [Verify the length of values](#verify-the-length-of-values)
  * [Verify the values are within a range](#verify-the-values-are-within-a-range) 
  * [Verify values are not in a list of invalid values](#verify-values-are-not-in-a-list-of-invalid-values-)
* [Allow some invalid values to occur, up to a threshold](#allow-some-invalid-values-to-occur-up-to-a-threshold)
* [Verify more specific validity on a subset of the data](#verify-more-specific-validity-on-a-subset-of-the-data)
* [Missing values are excluded](#missing-values-are-excluded)
* [List of valid & invalid value configuration keys](#list-of-valid--invalid-value-configuration-keys) 

### Verify there are no invalid values

```yaml
dataset: dim_employee
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

Multiple configurations can be combined.

##### Verify values are in a fixed list of valid values

This allows the user to define a fixed list of values to define validity. 

> Limitation: It can be either a list of strings or a list of numbers.  So only text and numeric columns are supported for now.

```yaml
dataset: dim_employee
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
dataset: dim_employee
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
dataset: dim_employee
columns:
  - name: country
    checks:
      - invalid:
          valid_reference_data:
            dataset: ['proddb', 'refschema', 'country_codes']
            column: country_code
```

The `valid_reference_data`.`dataset` is the fully qualified name of the reference dataset.

> Note: `valid_reference_data` is (for now) only combinable with missing configurations.  Not with other 
> validity configurations

> Note: If the reference dataset is located in the same schema, then it is sufficient to 
> only provide the name of the dataset itself as a string eg `dataset: country_codes`

> Performance warning! Using this type of configuration has an on performance.  Whereas other 
> validity configurations can be computed in one pass together with all the other metrics, the `valid_reference_data`
> requires its own separate query per check which can make the contract verification run slower and cost more.

##### Verify the length of values

```yaml
dataset: dim_employee
columns:
  - name: job_code
    checks:
      - invalid:
          valid_length: 2
```

```yaml
dataset: dim_employee
columns:
  - name: job_code
    checks:
      - invalid:
          valid_min_length: 1
          valid_max_length: 3
```

##### Verify the values are within a range

```yaml
dataset: dim_employee
columns:
  - name: idea_score_pct
    checks:
      - invalid:
          valid_min: 0
          valid_max: 100
```

##### Verify values are not in a list of invalid values 

Configure a list of values that will be considered invalid.

```yaml
dataset: dim_employee
columns:
  - name: desk
    checks:
      - invalid:
          invalid_values: ['Cocobola', 'Rubber']
```

```yaml
dataset: dim_employee
columns:
  - name: location
    checks:
      - invalid:
          invalid_format: email
```

```yaml
dataset: dim_employee
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
dataset: dim_employee
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
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: invalid
        metric: percent
        threshold:
          must_be_between: [0, 1]
```

The metric used in this check type is `invalid_percent`, which is calculated 
as: `invalid_count` x `100` / `row_count`

> Note: In case there are no rows, the invalid `percent` is considered 0 to 
> avoid a division by zero.     

For more details on threshold, see [Thresholds](thresholds.md) 

### Missing values are excluded

All the missing values configurations mentioned in [missing the values check](missing_checks.md#configure-extra-missing-values)
can also be specified here in the invalid check type.  Missing values will be excluded from the invalid values.  

When customizing the missing values beyond NULL, note that these custom missint values are also excluded from the validity checks.
This ensures that missing values are not counted double: `missing_count` + `invalid_count` + `valid_count` must be equal to `row_count`

Same reasoning: `missing_percent` + `invalid_percent` + `valid_percent` = 100

### List of valid & invalid value configuration keys

| Key                     | Description                            | Examples        |
|-------------------------|----------------------------------------|-----------------|
| `invalid_values`        | A list of values considered invalid    | ['X', 'ERROR']  |
| `invalid_format`        | A regex format                         |                 |
| `valid_values`          | A list of the valid values.            | ['S', 'M', 'L'] |
| `valid_format`          | A regex format                         |                 |
| `valid_min`             | The minimum valid value for the column |                 |
| `valid_max`             | The maximum valid value for the column |                 |
| `valid_length`          | TODO                                   |                 |
| `valid_min_length`      |                                        |                 |
| `valid_max_length`      |                                        |                 |
| `valid_reference_data`  |                                        |                 |
