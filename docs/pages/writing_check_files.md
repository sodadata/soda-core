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
          threshold:
            metric: percent
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

### Next: Adding checks

Please refer to these pages for adding checks to a SodaCL contract YAML file

* [schema](schema_check.md)
* [row_count](row_count_check.md)
* [missing_count & missing_percent](missing_checks.md)
* [invalid_count & invalid_percent](invalid_checks.md)
