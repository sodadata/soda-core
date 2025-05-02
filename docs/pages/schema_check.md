# Schema check

### Verify the schema of a dataset

The schema refers to the list of columns and their data types.

To verify the schema, add a check with `schema:` to the list of checks.
The list of column must be exact as specified in the contract.  
No extra nor missing columns are allowed and the order of the columns 
has to be like in the data source.

Column names are case sensitive.

For example:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    data_type: varchar
  - name: last_name
    data_type: varchar
  - name: address_line1

checks:
  - schema:
```

The above check will verify that the table `dim_employee` has exact 
3 columns named `id`, `last_name` and `address_line1`.  For columns `id`
and `last_name`, the warehouse data type has to match the provided, 
data-source-specific data types.

There are no further configuration keys for the schema check.

### Ignore extra columns

If you want to allow for extra columns, use `allow_extra_columns: true`

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
  - name: last_name

checks:
  - schema:
      allow_extra_columns: true
```

### Ignore column ordering

If you want to allow for columns to occur in a different order, use `allow_other_column_order: true`

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: last_name
  - name: address_line1
  - name: id

checks:
  - schema:
      allow_other_column_order: true
```

### Data type matching 

Column `data_type` keys are optional.  Only if the `data_type` key is specified on 
the column, the data type is checked as part of the schema check.

Data types specified in the contract will be checked case insensitive.

### Data type synonyms

Data types may support synonyms.  Eg on postgres, `data_type: varchar` will be equal 
to a column with metadata type `data_type: character varying`. 

### Specify and verify character maximum length

Data sources may support the optional column key `character_maximum_length`.
If specified in the contract on the column level, the schema check will 
verify it.  This is mostly for `varchar` data types that have been created with 
for example `varchar(255)`

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

columns:
  - name: id
    data_type: varchar
    character_maximum_length: 255
  - name: address_line1

checks:
  - schema:
```
