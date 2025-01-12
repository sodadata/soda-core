# Missing and validity configurations

### Column default configuration

There are missing value configuration keys 

| Key                 | Description                                           | Examples                 |
|---------------------|-------------------------------------------------------|--------------------------|
| `missing_values`    | A list of values that represent missing data          | ['N/A', '-', 'No value'] |
| `missing_regex_sql` | A warehouse SQL regex that matches for missing values | ^(-)+$                   |

Check types `missing_count` and `missing_percent` have missing configurations.

And there are validity configuration keys



| Key                     | Description                                                                                                                                      | Examples         |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|------------------|
| `invalid_values`        | A list of values considered invalid                                                                                                              | ['X', 'ERROR']   |
| `invalid_format`        | A format name that represents invalid values. [Formats are configured in the data source](data_source_configuration_reference.md#format-regexes) | email            |
| `invalid_regex_sql`     | A regex that matches invalid values. The regex is interpreted by the data source SQL engine.                                                     | ^X[a-Z]*$        |
| `valid_values`          | A list of the valid values.                                                                                                                      | ['S', 'M', 'L']  |
| `valid_format`          | A format name that represents valid values. [Format are configured in the data source](data_source_configuration_reference.md#format-regexes)    |                  |
| `valid_regex_sql`       | A regex that matches valid values. The regex is interpreted by the data source SQL engine.                                                       | ^X[a-Z]*$        |
| `valid_min`             | The minimum valid value for the column                                                                                                           |                  |
| `valid_max`             | The maximum valid value for the column                                                                                                           |                  |
| `valid_length`          |                                                                                                                                                  |                  |
| `valid_min_length`      |                                                                                                                                                  |                  |
| `valid_max_length`      |                                                                                                                                                  |                  |
| `valid_reference_data`  |                                                                                                                                                  |                  |

Check types `invalid_count` and `invalid_percent` take into account both validity and also missing configurations. 

### Column defaults and overwriting

Default missing and validity configurations can be specified on the check level.  All checks will then 
use the column missing and validity configurations as default

Missing and validity configurations on the column: 
```yaml
columns:
    - name: id
      missing_values: ['-']
      valid_regex_sql: ^XX[A-Z]+$
      checks: 
          - type: missing_count
          - type: invalid_count
```

In the above example, the `missing_count` check will count NULL's and '-' as missing values.
The `invalid_count` check will exclude, missing values NULL & '-' and only count other non-missing values that 
are invalid according to the configured `valid_regex_sql`.

Validity configuration overwrite: 
```yaml
columns:
    - name: category_code
      valid_regex_sql: ^X[A-Z]*$
      checks: 
          - type: invalid_count
          - type: invalid_count
            valid_values: ['XA', 'XB', 'XC']
            filter_sql: "country" = 'UK' 
```

In the example above, the first `invalid_count` check will use the column default validity configuration.
It will verify that each value in `category_code` is an `X` followed by any number of upper case letters.

The second check shows the typical use case for overwriting the default validity in combination with filters.
For a subset of the data, a more strict validity testing is verified.  In this case, the valid values are 
limited to `XA`, `XB`, `XC`

### Splitting metadata from checks

We foresee that a contract will be used to be the source of information for a data discovery tool.  Data 
discovery tools want to show all the properties of the data, with an indication if that property is 
verified with a check and if it passed the check.

For this reason, we have created the column defaults.  This allows users to communicate properties of the 
data to other tools and only separately make the decision to verify this in a check. 

Specify validity without verifying in a check 
```yaml
columns:
    - name: category_code
      valid_regex_sql: ^X[A-Z]*$
```

### Missing and validity sum

In order to ensure that `missing_count` + `invalid_count` + `valid_count` = `row_count`, 
the metrics and queries ensure that missing values are excluded when calculating the 
`invalid_count`. 

Same reasoning: `missing_percent` + `invalid_percent` + `valid_percent` = 100

### Roadmap

TODO: Create a `bad_count` metric/check: `bad_count` = `missing_count` + `invalid_count`
