Verify that the dataset must have rows
```yaml
dataset: DS
checks: 
  - row_count:
```

Verify that the row count must be between 100 and 500
```yaml
dataset: DS
checks: 
  - row_count:
      between: [100, 500]
```

Provide a user defined name or label for the row count check
```yaml
dataset: DS
checks: 
  - row_count:
      with_name: Row count 
      between: [100, 500]
```
