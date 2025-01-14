Schema check verifying exact match on column names and verifying data types if they are specified.
```yaml
dataset: DS
columns:
    - name: id
      data_type: VARCHAR
    - name: name
      data_type: VARCHAR
    - name: size
    - name: age
checks: 
  - type: schema
```

Schema check verifying that at least the specified column names are present and for those that are present and specified, verify the data type matches.
```yaml
dataset: DS
columns:
    - name: id
checks: 
  - type: schema
    unexpected_columns_allowed: True
```

Force that each column must have a data type specified
```yaml
dataset: DS
columns:
    - name: id
      data_type: VARCHAR(255)
checks: 
  - type: schema
    data_types_required: True
```

Also verify the order/index of the columns 
```yaml
dataset: DS
columns:
    - name: id
checks: 
  - type: schema
    verify_column_order: True
```
