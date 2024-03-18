# Writing contract checks

* [Schema check](#schema-check)
* [Row count check](#row-count-check)
* [Missing values check](#missing-values-checks)
* [Invalid values check](#invalid-values-checks)
* [Uniqueness check](#uniqueness-check)
* [Freshness check](#freshness-check)
* [Basic SQL aggregation checks](#basic-sql-aggregation-checks)
* [Multi-column duplicates check](#multi-column-duplicates-check)
* [User-defined metric SQL expression check](#user-defined-sql-check)
* [User-defined metric SQL query check](#user-defined-sql-check)
* [User-defined failed rows SQL query check](#user-defined-sql-check)

> IDE code completion support: When authoring contract YAML files, consider using
> the [JSON schema file](../soda/contracts/soda_data_contract_json_schema_1_0_0.json)
> to get code completion and editor support in either [PyCharm](01_contract_basics#setting-up-code-completion-in-pycharm)
> or [VSCode](01_contract_basics#setting-up-code-completion-in-vscode)

### Schema check

When verifying a contract, Soda will first check the schema.  The schema is essential and will always be checked.

Example contract schema check:
```yaml
dataset: CUSTOMERS
columns:
  - name: id
    data_type: VARCHAR
  - name: size
  - name: discount
    optional: true
```

Optionally, if the `data_type` property is specified in the column, the data type will be checked as well as part of
the schema check.

By default all columns specified in the contract are required. Columns can also be marked as `optional: true`.

See the examples below for more schema features like optional columns and allowing other columns.

### Row count checks

Example: Simplest row count check. This dataset level check verifies at least 1 row exists.
```yaml
dataset: CUSTOMERS
checks:
  - type: rows_exist
```

Example: Specify a threshold for your `row_count`.  See [section Tresholds](#thresholds) to see all other
`must_...` threshold configurations.
```yaml
dataset: CUSTOMERS
checks:
  - type: row_count
    must_be_between: [100, 120]
```

> Tip: If you have configured the JSON schema in your YAML editor, you can start typing key `must_` in the
> check and use code completion to find the right syntax of the [threshold](#thresholds) you want.

Missing values also can have the [common check properties](#common-check-properties)

### Missing values checks

There are 3 missing check types:

| Missing check type | Threshold requirement                                         | Check fails when                                                                                         |
|--------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| no_missing_values  | No `must_...` threshold keys allowed                          | There are missing values                                                                                 |
| missing_count      | At least one `must_...` [threshold](#thresholds) key required | The number of missing values does not exceed to the specified threshold                                  |
| missing_percent    | At least one `must_...` [threshold](#thresholds) key required | The percentage of missing values relative to the total row count does not exceed the specified threshold |

Example: Easy not-null check type. `no_missing_values` is a short version of missing_count must be zero.
This check verifies that there are no NULL values in CUSTOMERS.id.  There is no need to specify a must_... threshold,
which makes it easy.

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: no_missing_values
```

Example: Use check type `missing_count` to configure a different threshold with one of the `must_...` configurations.
This check verifies there are less than 10 NULL values in CUSTOMERS.id

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: missing_count
        must_be_less_than: 10
```

Example: Customize the values considered missing with `missing_values`. This check verifies there are no missing values
in CUSTOMERS.id where NULL, `'N/A'` and `'No value'` are considered missing values.  `NULL` is always considered
as missing so that does not have to be specified in the list.

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: no_missing_values
        missing_values: ['N/A', 'No value']
```

Example: Configure optional `missing_sql_regex`.  This check verifies there are no missing values in CUSTOMERS.id where
missing values are specified with a SQL regex. The regex is directly used in the SQL query so it has to match the
dialect of your SQL-engine.

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: no_missing_values
        missing_sql_regex: '^[# -]+$'
```

Missing values also can have the [common check properties](#common-check-properties)

### Validity checks

| Missing check type | Threshold requirement                          | Check fails when                                                                                         |
|--------------------|------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| no_invalid_values  | No `must_...` threshold keys allowed           | There are invalid values                                                                                 |
| invalid_count      | At least one `must_...` threshold key required | The number of invalid values does not exceed to the specified threshold                                  |
| invalid_percent    | At least one `must_...` threshold key required | The percentage of invalid values relative to the total row count does not exceed the specified threshold |

Validity checks always require a validity configuration.  The complete list of validity configurations is

| Validity configuration key    |
|-------------------------------|
| `valid_values`                |
| `valid_values`                |
| `valid_format`                |
| `valid_sql_regex`             |
| `valid_min`                   |
| `valid_max`                   |
| `valid_length`                |
| `valid_min_length`            |
| `valid_max_length`            |
| `valid_values_reference_data` |
| `invalid_values`              |
| `invalid_format`              |
| `invalid_sql_regex`           |

> Tip: If you have configured the JSON schema in your YAML editor, you can start typing keys `valid` in the
> check and use code completion to find the right syntax of these validity configurations

This first example shows the simplest and most common invalid check type with a configured list of valid values.
The check will fail if there is a value for `size` is not missing (NULL by default) and not in the given list.

```yaml
dataset: CUSTOMERS
columns:
  - name: size
    checks:
      - type: no_invalid_values
        valid_values: ['S', 'M', 'L']
```

Example check verifies the validity of a SQL using a regular expression. The regex is directly used in the SQL
query so it has to match the dialect of your SQL-engine.

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: invalid_count
        valid_sql_regex: '^ID.$'
```

Example of a check that verifies that each value is between certain range of values.  This check assumes the column has a
numeric data type. The min and max boundary values are considered ok.

Here we see an example of combining multiple validity configurations.  **All** the specified validity configurations have to be
met for a value to be valid.  So in other words: `AND` logic is applied.

```yaml
dataset: CUSTOMERS
columns:
  - name: market_share_pct
    checks:
      - type: invalid_count
        valid_min: 0
        valid_max: 100
        must_be_less_than: 1000
```

Example of valid length checks as a range.  This check verifies that values have a length between a min and max length.
The min and max boundary values are considered ok.

```yaml
dataset: CUSTOMERS
columns:
  - name: comment
    checks:
      - type: no_invalid_values
        valid_min_length: 1
        valid_max_length: 144
```

Example of a check verifying that each value has a fixed length of 5. This assumes the column is a text or other type that supports
the `length(...)` SQL function.

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: no_invalid_values
        valid_length: 5
```

Example of a reference data validity check. This counts all values that occur in the
column and that do not occur in the `valid_values_reference_data` column. This is also
known as referential integrity or foreign key check.

> Performance tip: This check is a bit special in the senses that it requires a separate query. Other validity
> checks are expressions that are combined in a single query for performance.  That's not possible for the
> reference data check.

```yaml
dataset: CUSTOMERS
columns:
 - name: category_id
   checks:
     - type: no_invalid_values
       valid_values_reference_data:
         dataset: CUSTOMER_CATEGORIES
         column: id
```

Example of combing missing & invalid. Soda's contract engine is cleverly separating the missing values from the
computation of the invalid values.  It ensures that missing values are not double counted as invalid values and that
the sum of missing values + invalid values + valid values adds up to the row count.

```yaml
dataset: CUSTOMERS
columns:
  - name: size
    checks:
      - type: no_invalid_values
        missing_values: ['N/A']
        valid_values: ['S', 'M', 'L']
```

Even more clever, Soda's parser leverages any previous missing and valid configurations for subsequent checks.
In the next example, the `no_invalid_values` check will leverage the `missing_values` configuration from the previous
check.

> Detail: In the (unlikely) case that there are multiple missing checks with missing values configs, they are
> overriding (not merging). Last one wins.

> Caveat: This ignoring of missing values probably doesn't work when using 'valid_values_reference_data' configuration

```yaml
dataset: CUSTOMERS
columns:
  - name: size
    checks:
      - type: no_missing_values
        missing_values: ['N/A']
      - type: no_invalid_values
        valid_values: ['S', 'M', 'L']
```

### Duplicate / uniqueness checks

This is also known as uniqueness checks.  In the Soda contract language, for consistently, we only use the notion of counting
duplicates instead of uniqueness.  That allows for ranges that allow flexibility to relax the uniqueness constraint in cases
where that is applicable.

| Missing check type  | Threshold requirement                                           | Check fails when                                                                                           |
|---------------------|-----------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| no_duplicate_values | No `must_...` threshold keys allowed                            | There are duplicate values                                                                                 |
| duplicate_count     | At least one `must_...` [threshold](#thresdholds) key required  | The number of duplicate values does not exceed to the specified threshold                                  |
| duplicate_percent   | At least one `must_...` [threshold](#thresdholds) key required  | The percentage of duplicate values relative to the total row count does not exceed the specified threshold |

This example shows a simple uniqueness check

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: no_duplicate_values
```

Example of a check specifying a threshold on the `duplicate_count`

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: duplicate_count
        must_be_less_than: 10
```

Example of a check specifying a threshold on the `duplicate_percent`

```yaml
dataset: CUSTOMERS
columns:
  - name: id
    checks:
      - type: duplicate_percent
        must_be_less_than: 1
```

The next example shows a multi columns duplicates / uniqueness check.  In this case the check has to be specified
on the dataset level.  The 3 check types `no_duplicate_count`, `duplicate_count` and `duplicate_percent` all support
the `columns` key on the dataset level.

```yaml
dataset: CUSTOMERS
checks:
  - type: no_duplicate_count
    columns: ['country_code', 'zip']
```

### Freshness checks

Checks if there are rows indicating recent data has been added.  It assumes there is a column that represents a timestamp like
an event time or so.  The check looks for the maximum value in the column and verifies if that maximum value is not older than
a given threshold time period.

| All freshness check types    |
|------------------------------|
| `type: freshness_in_days`    |
| `type: freshness_in_hours`   |
| `type: freshness_in_minutes` |

Example freshness check

```yaml
dataset: CUSTOMERS
checks:
  - type: freshness_in_hours
    must_be_less_than: 6
```

### Basic SQL aggregation checks

Example of check verifying that the average of column `size` must be between 10 and 20

```yaml
dataset: CUSTOMERS
columns:
  - name: size
    checks:
      - type: avg
        must_be_between: [10, 20]
```

| Numeric SQL aggregation check types |
|-------------------------------------|
| `type: avg`                         |
| `type: sum`                         |

### User defined SQL checks

Example of a user-defined SQL expression check

```yaml
dataset: CUSTOMERS
columns:
  - name: country
    checks:
      - type: sql_expression
        metric: us_count
        sql_expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
        fail_when_not_between: [100, 120]
```

Example of a user-defined SQL query check

```yaml
dataset: CUSTOMERS
checks:
  - type: user_defined_sql
    metric: us_count
    sql_query: |
        SELECT COUNT(*)
        FROM {table_name}
        WHERE country = 'US'
    must_be_between: [0, 5]
```

### Filter

Sometimes new data is already appended in an incremental table and you only want to run the
contract checks on the new data.  For this, a SQL filter can be used.

The following check requires a variable to be passed.

```yaml
dataset: CUSTOMERS
sql_filter: |
  created > ${FILTER_START_TIME}
columns:
  - name: id
  - name: created
checks:
  - type: row_count
    must_be_greater_than: 10
```

### Thresholds

Some check types have default thresholds.  If you do want to specify a threshold, use one of these check configuration properties

| Threshold key                      | Value             | Example                                 |
|------------------------------------|-------------------|-----------------------------------------|
| `must_be`                          | Number            | `must_be: 0`                            |
| `must_not_be`                      | Number            | `must_not_be: 0`                        |
| `must_be_greater_than`             | Number            | `must_be_greater_than: 100`             |
| `must_be_greater_than_or_equal_to` | Number            | `must_be_greater_than_or_equal_to: 100` |
| `must_be_less_than`                | Number            | `must_be_less_than: 100`                |
| `must_be_less_than_or_equal_to`    | Number            | `must_be_less_than_or_equal_to: 100`    |
| `must_be_between`                  | List of 2 numbers | `must_be_between: [0, 100]`             |
| `must_be_not_between`              | List of 2 numbers | `must_be_not_between: [0, 100]`         |

> Tip: If you have configured the JSON schema in your YAML editor, you can start typing key `must_` in the
> check and use code completion to find the right syntax of the [threshold](#thresholds) you want.

When specifying ranges with `must_be_between` boundaries values are considered ok. In the example below,
`row_count`s of 100 and 120 both will pass.

```yaml
dataset: CUSTOMERS
checks:
  - type: row_count
    must_be_between: [100, 120]
```

When specifying ranges with `must_be_not_between` boundaries values are considered not ok. In the example below,
`row_count`s of 0 and 120 will both fail.

```yaml
dataset: CUSTOMERS
checks:
  - type: row_count
    must_be_not_between: [0, 120]
```

To tweak the inclusion of boundary values, use a combination of the less than and greater than keys.

The next example check passes if the avg < 100 or avg >= 200. In this case, values outside the range are ok.

```yaml
dataset: CUSTOMERS
checks:
  - type: avg
    must_be_less_than: 100
    must_be_greater_than_or_equal_to: 200
```

The next example check passes if 100 <= avg < 200. In this case, values inside the range are ok.

```yaml
dataset: CUSTOMERS
checks:
  - type: avg
    must_be_greater_than_or_equal_to: 100
    must_be_less_than: 200
```

### Common check properties

| Key  | Description                           |
|------|---------------------------------------|
| Name | The human readable name for the check |
