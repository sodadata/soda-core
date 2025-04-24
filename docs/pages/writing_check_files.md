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

In a contract, you can refer to variables using the syntax `${var.VARNAME}`, The variable reference 
will be replaced with the variables value.

Example
```yaml
dataset: postgres_adventureworks/adventureworks/${var.DATASET_SCHEMA}/${var.DATASET_PREFIX}_employee

variables: 
  DATASET_SCHEMA:
    default: advw
  DATASET_PREFIX:

columns:
  - name: id
```

Variable values are specified in one of 3 ways: 
* Provided as parameter when starting a contract verification
* Using the `default` key in the variable declaration in the contract YAML

Variable declarations optionally can have a `default` value assigned.  The default variable 
values are allowed to contain nested variable references.  But of course, no circular 
references are allowed.

Variable are resolved case sensitive.

In contract YAML files, environment variables like eg `${env.ERROR}` can **not** be used.

### Building a dataset filter with time partition variables

Use `filter` to apply the checks to a filtered set of records in the dataset.

The `filter` is a SQL expression that is appended to the WHERE-clause of the 
generated queries.  It's used mostly to filter the slice of data tested with the contract 
for the latest time partition or for a particular sub category of the data.

Use `${soda.NOW}` to get a timestamp that is initialized on the current time.  
It's formatted in ISO 8601 standard.

The `soda` namespace only has constants and values can not be over overwritten.

Use variables and `${soda.NOW}` to build a time partition filter:
```yaml
dataset: postgres_test_ds/soda_test/dev_tom/SODATEST_filter_1cac5827

variables:
  START_TS:
    default: date_trunc('day', timestamp '${soda.NOW}')
  END_TS:
    default: ${var.START_TS} + interval '1 day'
  TIME_PARTITION_FILTER:
    default: |
      ${var.START_TS} < "updated"
      AND "updated" <= ${var.END_TS}

filter: ${var.TIME_PARTITION_FILTER}

checks:
  - failed_rows:
      query: |
        SELECT * 
        FROM "adventureworks"."advw"."dim_employee"
        WHERE SOME_OTHER < CRITERIA
          AND ( ${var.USER_DEFINED_FILTER_VARIABLE} )
```

In the example above you can also see the `${var.TIME_PARTITION_FILTER}` being used in user defined SQL queries.

The postgres functions `date_trunc` is used to compute the previous midnight.  And the 
`+ interval '1 day'` is used to compute 24 hours later than the start timestamp.

NOTE: The `failed_rows` check type shown in the next example is not yet supported.

### Check filters

Every check can have a filter that limits the rows it applies to with a SQL expression filter.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    checks:
      - missing:
          filter: |
            "country" = 'USA'
```

Check types that support filters: `missing`, `invalid`, `row_count`

Check filters can be combined with dataset level filters. 

### Next: Adding checks

Please refer to these pages for adding checks to a SodaCL contract YAML file

* [schema](schema_check.md)
* [row_count](row_count_check.md)
* [missing_count & missing_percent](missing_checks.md)
* [invalid_count & invalid_percent](invalid_checks.md)
