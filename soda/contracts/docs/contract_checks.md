


## File naming convention

We recommend that to use the extension `.sdc.yml` for all Soda data contract files.
This will make it easier for tools to associate the contract files with the 
appropriate schema file to get better editing experience in your IDE.

## Schema verification

When verifying a contract, Soda will first verify the schema.  The schema check will 
check the presence of columns and optionally their data type.

By default all columns specified in the contract are required and no other columns are allowed. 

Optionally, if the `data_type` property is specified in the column, the data type will be checked as well as part of
the schema check.

See the examples below for more schema features like optional columns and allowing other columns.

```yaml
dataset: CUSTOMERS
columns:
    - name: id
      data_type: VARCHAR
    - name: size
```

See [more schema examples](EXAMPLES.md#schema-examples) 

## Column basic check configurations

On each column, a limited set of basic check types can be configured with some s can be configured with a short style.

```yaml
dataset: CUSTOMERS
columns:
    - name: id
      checks:
        - type: no_invalid_values
          valid_format: uuid
        - type: unique
    - name: size
      checks:
        - type: no_invalid_values
          valid_values: ['S','M','L']
```

See [more basic column check configuration examples](EXAMPLES.md#basic-column-check-configuration-examples) 

## Column checks

In addition to the basic check configurations, on each column, a list of Soda checks can be configured as well. Eg

See [more column check examples](EXAMPLES.md#column-check-examples) 


## Dataset checks

Typical dataset checks are row_count (aka volume) and freshness.

```yaml
dataset: CUSTOMERS
checks: 
    - type: row_count
      fail_when_is: 0
    - type: freshness_in_hours
      fail_when_greater_than: 6
```


# Schema examples

The `name` property of each column is **required**, it has to match with the name in the SQL engine.

Property `data_type` is optional.  Only when specified, it is verified.

```yaml
dataset: CUSTOMERS
columns:
    - name: id
      data_type: VARCHAR
    - name: size
```

The name has to match with the actual column name in the SQL engine.

# Basic column check configuration examples

# Column check examples

# Dataset check examples
