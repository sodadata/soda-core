# Writing check files

### Contract YAML layout

Example contract YAML file:
```
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
  - name: last_name
    checks:
      - missing:
          metric: percent
          threshold:
            must_be_less_than: 10
  - name: address_line1
    checks:
      - missing:
          missing_values: ['N/A', 'No value', '-']
          threshold:
            must_be_between: [0, 10]

checks:
  - schema:
  - row_count:
      threshold:
        must_be_between: [10, 100]
```

### Contract variables

Each variable that is used in a contract in the `variables` section:

```yaml
dataset: postgres_adventureworks/adventureworks/${DATASET_SCHEMA}/${DATASET_PREFIX}_employee

variables: 
  DATASET_SCHEMA:
    default: advw
  DATASET_PREFIX:
    default: dim

columns:
  - name: id
```

In a contract, you can refer to variables using the syntax `${var.VARNAME}`, The variable reference 
will be replaced with the values specified in the CLI, the Python API or the Soda Cloud UI.

Variable are resolved case sensitive.

In contract YAML files, environment variables like eg `${env.ERROR}` can **not** be used.

All variables used in a contract except for `NOW` have to be declared.

`NOW` has the current timestamp in ISO8601 format as default.  A value for `NOW` can be 
provided in the variables, but then it has to be also in ISO8601 format.  Variable can 
optionally be declared and given a default value, but doesn't have to be.

### Checks filter

Use `checks_filter` to apply the checks to a filtered set of records in the dataset.

The `checks_filter` is a SQL expression that is appended to the WHERE-clause of the 
generated queries.  It's used mostly to filter the slice of data tested with the contract 
for the latest time partition or for a particular sub category of the data.

The following example shows how a time partition filter for 'today' based on the `NOW` variable 
using postgres SQL functions.  The `NOW` variable is automatically initialized by the contract 
verification.

The postgres functions `date_trunc` is used to compute the previous midnight.  And the 
`+ interval '1 day'` is used to compute 24 hours later than the start timestamp.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

checks_filter: |
    date_trunc('day', timestamp '${var.NOW}') < {column_name_quoted}
    AND {column_name_quoted} <= date_trunc('day', timestamp '${var.NOW}') + interval '1 day'

columns:
  - name: country
    checks:
      - missing:
```

Use a variable to leverage the checks filter also in other user defined SQL expressions 
elsewhere in the contract:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

variables:
  START_TS:
    default: {start_ts_value}
  END_TS:
    default: {end_ts_value}
  USER_DEFINED_FILTER_VARIABLE:
    default: |
      ${{var.START_TS}} < {column_name_quoted}
      AND {column_name_quoted} <= ${{var.END_TS}}

checks_filter: ${{var.USER_DEFINED_FILTER_VARIABLE}}

checks:
  - failed_rows:
      query: |
        SELECT * 
        FROM "adventureworks"."advw"."dim_employee"
        WHERE SOME_OTHER < CRITERIA
          AND ( ${var.USER_DEFINED_FILTER_VARIABLE} )
```

### Next: Adding checks

Please refer to these pages for adding checks to a SodaCL contract YAML file

* [schema](schema_check.md)
* [row_count](row_count_check.md)
* [missing_count & missing_percent](missing_checks.md)
* [invalid_count & invalid_percent](invalid_checks.md)
