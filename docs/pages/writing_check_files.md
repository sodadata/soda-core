# Writing check files

### Contract YAML layout

Example contract YAML file:
```
dataset: dim_employee
columns:
  - name: id
    checks:
      - type: missing_count
  - name: last_name
    checks:
      - type: missing_percent
        must_be_less_than: 10
  - name: address_line1
    missing_values: ['N/A', 'No value', '-']
    checks:
      - type: missing_count
        must_be_between: [0, 10]

checks:
  - type: schema
  - type: row_count
    must_be_between: [10, 100]
```

### Linking contracts to the data source

In your source code repository, we recommend to use a top level directory called `soda` and 
organize all the soda YAML configuration files in there.  

### Next: Adding checks

Please refer to these pages for adding checks to a SodaCL contract YAML file

* [schema](schema_check.md)
* [row_count](row_count_check.md)
* [missing_count & missing_percent](missing_checks.md)
* [invalid_count & invalid_percent](invalid_checks.md)
