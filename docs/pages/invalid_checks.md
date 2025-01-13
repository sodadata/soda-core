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
    valid_values: ['S', 'M', 'L']
    checks:
      - type: invalid_count
```

> Note: The default threshold is that `invalid_count` must be 0.
 
> Note: See [Configure invalid values](#configure-invalid-values) for other ways to configure valid values. 

> Note: Check out [the note below](#config_level_note) that explains why column level validity configuration as used 
> in the above example is preferred over check level configurations.

### Configure invalid values

##### Verify values are in a fixed list of valid values

```yaml
dataset: dim_employee
columns:
  - name: size
    valid_values: ['S', 'M', 'L']
    checks:
      - type: invalid_count
```

##### Verify values are in a named format

```yaml
dataset: dim_employee
columns:
  - name: email_address
    valid_format: email
    checks:
      - type: invalid_count
```

The valid format name `email` in the example above must be configured in [the data source configuration file](data_source.md#format-regexes).

On Soda Cloud, the editor will help by showing the full list of options. 

##### Verify valid values with a regex 

```yaml
dataset: dim_employee
columns:
  - name: job_code
    valid_regex_sql: ^XX[0-9]{4}$
    checks:
      - type: invalid_count
```

The regex expression is passed on as-is in the SQL query so the syntax has to match 
with the regex syntax used by the data source SQL engine. 

##### Verify valid values occur in a reference dataset

```yaml
dataset: dim_employee
columns:
  - name: country
    valid_reference_data:
      dataset: ['proddb', 'refschema', 'country_codes']
      column: country_code
    checks:
      - type: invalid_count
```

The `valid_reference_data`.`dataset` is the fully qualified name of the reference dataset. 

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
    valid_length: 2
    checks:
      - type: invalid_count
```

```yaml
dataset: dim_employee
columns:
  - name: job_code
    valid_min_length: 1
    valid_max_length: 3
    checks:
      - type: invalid_count
```

> Note: The valid length configurations can be used in combination with all other validity configurations except `valid_reference_data` 

##### Verify the values are within a range

```yaml
dataset: dim_employee
columns:
  - name: idea_score_pct
    valid_min: 0
    valid_max: 100
    checks:
      - type: invalid_count
```

##### Verify values are not in a list of invalid values 

```yaml
dataset: dim_employee
columns:
  - name: desk
    invalid_values: ['Cocobola', 'Rubber']
    checks:
      - type: invalid_count
```

```yaml
dataset: dim_employee
columns:
  - name: location
    invalid_format: email
    checks:
      - type: invalid_count
```

```yaml
dataset: dim_employee
columns:
  - name: desk
    invalid_regex_sql: ^a[16]z$
    checks:
      - type: invalid_count
```

> Note: The invalid value configurations can be used in combination with all other validity configurations except `valid_reference_data` 

> <a name="config_level_note"></a> Note: `valid_values` is on the column level. That's because of 2 reasons:
> * Multiple checks will use the `valid_values` configuration.  This way you don't have to 
>   repeat the missing values configuration and maintain the duplicate configurations. 
> * It separates the metadata (information describing the column) from the actual check.
> Valid values can also be [specified or overwritten on the check level](#).

### Allow some invalid values to occur, up to a threshold

Verify there are less than 25 invalid values in a column:

```yaml
dataset: dim_employee
columns:
  - name: size
    valid_values: ['S', 'M', 'L']
    checks:
      - type: invalid_count
        must_be_less_than: 25
```

Verify there are between 0 and 1 % missing values in a column:

```yaml
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: invalid_percent
        must_be_between: [0, 1]
```

The metric used in this check type is `invalid_percent`, which is calculated 
as: `invalid_count` x `100` / `row_count`

> Pro tip: It can be more informative to use invalid_percent as the diagnostic 
> information for this check will include the actual row_count, missing_count and 
> missing_percentage values. But...

> Warning: A `invalid_percent` check can only be evaluated if there are rows.
> That's because division by zero is not possible.  A `invalid_percent` check 
> will have an outcome value of UNEVALUATED in case there are no rows. This 
> will cause the contract verification to fail while there are no rows with 
> missing values.   

For more details on threshold, see [Thresholds](thresholds.md) 

### Verify more specific validity on a subset of the data

Overwrite the column validity on a validity check
```
dataset: dim_employee
columns:
  - name: zip
  
    # Specifies the default column validity:
    valid_regex_sql: ^[0-9A-Z ]{4,10}$
    
    checks:

      # Check the generic column validity on all data:
      - type: invalid_count

      # Check the specific validity only on the slice for Belgium:
      - type: invalid_count
        valid_regex_sql: ^[0-9]{4,4}$
        data_filter_sql: country = 'BE' 
```

> Note: Once a check overwrites any validity configuration, none of the validity keys in the column are taken into account.
> If you overwrite the `valid_min_length` in the check, you also have to overwrite the `valid_min_max`.  Similarly, 
> if you configure a `valid_values` in the checks, any other valid configuration on the column level like 
> eg `valid_regex_sql` is ignored. 

### Missing values are excluded

When customizing the missing values beyond NULL, note that these custom missint values are also excluded from the validity checks.
This ensures that missing values are not counted double: `missing_count` + `invalid_count` + `valid_count` must be equal to `row_count`

Same reasoning: `missing_percent` + `invalid_percent` + `valid_percent` = 100

### List of valid & invalid value configuration keys

| Key                     | Description                                                                                                                                      | Examples         |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `invalid_values`        | A list of values considered invalid                                                                                                              | ['X', 'ERROR']   |
| `invalid_format`        | A format name that represents invalid values. [Formats are configured in the data source](data_source#format-regexes) | email            |
| `invalid_regex_sql`     | A regex that matches invalid values. The regex is interpreted by the data source SQL engine.                                                     | ^X[a-Z]*$        |
| `valid_values`          | A list of the valid values.                                                                                                                      | ['S', 'M', 'L']  |
| `valid_format`          | A format name that represents valid values. [Format are configured in the data source](data_source#format-regexes)    |                  |
| `valid_regex_sql`       | A regex that matches valid values. The regex is interpreted by the data source SQL engine.                                                       | ^X[a-Z]*$        |
| `valid_min`             | The minimum valid value for the column                                                                                                           |                  |
| `valid_max`             | The maximum valid value for the column                                                                                                           |                  |
| `valid_length`          |                                                                                                                                                  |                  |
| `valid_min_length`      |                                                                                                                                                  |                  |
| `valid_max_length`      |                                                                                                                                                  |                  |
| `valid_reference_data`  |                                                                                                                                                  |                  |
