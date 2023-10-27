# Goal and purpose of data contracts in Soda

Soda's goal is to be the best enforcement engine for data contract implementations. For
this purpose we have created a new YAML format that aligns with data contracts from a producer
perspective.  This new format enables a very easy translation from any data contract
YAML language to Soda's enforcable contract language.

# Example

```yaml
dataset: DIM_CUSTOMER

columns:
  - name: id
    data_type: character varying
    unique: true
  - name: cst_size
    data_type: decimal
  - name: cst_size_txt
    valid_values: [1, 2, 3]
  - name: distance
    data_type: integer
  - name: country
    data_type: varchar
    not_null: true
  - name: ts

checks:
  - avg(distance) between 400 and 500
```

# Top level keys

| Key | Description | YAML data type |
| --- | ----------- | -------------- |
| `dataset` | Name of the dataset as in the SQL engine. | string |
| `columns` | Schema specified as a list of columns.  See below for the column format | list of objects |
| `checks` | SodaCL checks.  See [SodaCL docs](https://docs.soda.io/soda-cl/metrics-and-checks.html) | list of SodaCL checks |

# Column keys

| Key | Description | YAML data type |
| --- | ----------- | -------------- |
| `name` | Name of the column as in the SQL engine. | string |
| `data_type` | Name of the data type as in the SQL engine | string |
| `not_null` | `not_null: true` creates a missing values check | boolean |
| `unique` | `unique: true` creates a uniqueness check | boolean |
| `valid_values` | Triggers a validity check and specifies the list of allowed values | list of strings or numbers |
