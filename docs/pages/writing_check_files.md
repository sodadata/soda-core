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
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

variables: 
  COUNTRY:
    default: USA
  CATEGORY:
    default: L
  CREATED_AFTER_TS:
  SEGMENT_FILTER:
    default: |
        country ='${var.COUNTRY}' 
        AND category = '${var.CATEGORY}' 
        AND created_at <= TIMESTAMP '${var.NOW}
        AND created_at > TIMESTAMP '${var.CREATED_AFTER_TS}

filter: ${var.SEGMENT_FILTER}
```

In a contract, you can refer to variables using the syntax `${var.VARNAME}`, The variable reference 
will be replaced with the values specified in the CLI, the Python API or the Soda Cloud UI.

Variable are resolved case sensitive.

In contract YAML files, environment variables like eg `${env.ERROR}` can **not** be used.

All variables used in a contract except for `NOW` have to be declared.

`NOW` has the current timestamp in ISO8601 format as default.  A value for `NOW` can be 
provided in the variables, but then it has to be also in ISO8601 format.  Variable can 
optionally be declared and given a default value, but doesn't have to be.

### Next: Adding checks

Please refer to these pages for adding checks to a SodaCL contract YAML file

* [schema](schema_check.md)
* [row_count](row_count_check.md)
* [missing_count & missing_percent](missing_checks.md)
* [invalid_count & invalid_percent](invalid_checks.md)
