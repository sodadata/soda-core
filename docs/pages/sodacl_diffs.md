# Changes between SodaCL and Contract YAML language

### 100% coverage in the Soda Cloud editor

The Soda Cloud editor will cover 100% of the Soda Contract language.

### JSONSchema

The contract YAML language has a JSON Schema that drives code completion in the Soda Cloud contract editor and IDEs.

For this, the compact and complex syntax of SodaCL is replaced with a syntax that has no variables in the keys and where all values are atomic.

### No warnings

⟶ Data testing is more about stopping the pipeline and notifications.

Motivation: Previously, we were mixing monitoring and observability capabilities in the check files. Now we are building a clear separation and working towards 2 separate products for data testing and observability.

For data testing the main goal is to build it into the pipelines as unit tests. If we add warnings, then we also have to allow for configuration and explain how users have to map this to stopping the pipeline or not. That model is too complex. Simpler is: testing is only pass or fail and you stop the pipeline if either checks fail or the contract verification has execution errors.

### No change over time

Motivation: Similar as above, this will move into metric monitoring and observability

#### Check type as the key

```yaml
columns:
  - name: id
    checks:
      - missing:
      - duplicate:
```

Given that all nested configurations for these checks have good defaults, these checks above are fully configured.

### Merging the *count &* \_\_percent checks

We want to unify the check types. Instead of splitting up check types by metric, we only have 1 check type for missing, invalid and duplicate.

`missing_count` & `missing_percent` ==> `missing`

`invalid_count` & `invalid_percent` ==> `invalid`

`duplicate_count` & `duplicate_percent` ==> `duplicate`

An additional nested property `metric` will be added to allow users configure `count` or `percent`

```yaml
columns:
  - name: id
    checks:
      - missing:
          metric: percent
      - duplicate:
          metric: percent
```

#### Grouping the threshold keys

`threshold` will group all the threshold information

```yaml
columns:
  - name: id
    checks:
      - missing:
          threshold:
            must_be_greater_than: 5
```

#### Metadata vs checks

> Note: This shows a potential direction.  For now, check configurations are local and not yet reusable on the column level. 

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    unique: true
    checks:
      - duplicate:
```

`unique: true` indicates that the column must be unique. This is generic metadata that is part of "the API for data". In the sense that might or might not be used in checks. This information can be sent to a data discovery tool like a catalog where consumer will learn that the producer states this is a column with unique values.

The `duplicate:` check is a check and part of a check suite which verifies that this column does not contain duplicates (within the active filter if present)

Similarly, validity and missing configurations both have their API documentation part on the column and the check logic separated:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    not_null: true
    missing_values: ["N/A", ""]
    valid_format:
      regex: ^XX[0-9]{4}$
      name: XX-number
    checks:
      - missing:
      - invalid:
```

### Check identities

The parser will ensure that all checks have a unique identity within the contract file. Even in local execution.

The identity of the check will be auto-unique as long as it's the only check of the given type in that checks section.

Identity will be composed of the following parts:

* Data source name
* Dataset prefix
* Dataset name
* Column name (if applicable)
* Check type
* Check `qualifier` property (user defined to make identity unique within the `checks` section)

If you want 2 checks of the same type in the same `checks` section, you have to provide a `qualifier` property

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
checks:
  - row_count:
  - row_count:
      qualifier: 2
      threshold:
        must_be_greater_than: 10
```
