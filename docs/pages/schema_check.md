# Schema check

### Basic schema check

To verify the schema, add a check with `type: schema` to the list of checks.
This is the most common check. The list of column must be exact as 
specified in the contract.  No extra nor missing columns are allowed.  
Column names are case sensitive.

For example:

```
dataset: dim_employee
columns:
  - name: id
    data_type: varchar
  - name: last_name
    data_type: varchar
  - name: address_line1

checks:
  - type: schema
```

The above check will verify that the table `dim_employee` has exact 
3 columns named `id`, `last_name` and `address_line1`.  For columns `id`
and `last_name`, the warehouse data type has to match the provided, 
data-source-specific data types.

There are no further configuration keys for the schema check.

### Data type matching 

Column `data_type` keys are optional.  Only if the `data_type` key is specified on 
the column, the data type is checked as part of the schema check.

Data types specified in the contract will be checked case insensitive.

Data types may support synonyms.  Consult the specific data source 
for details (TODO).  Like eg in postgres, `data_type: varchar` will be equal 
to a column with metadata type `character varying`. 

Data sources may support max lengths like `data_type: varchar(255)`.  
Lengths are considered optional.  They are only checked if specified in the 
contract with round brackets.  So for a column with data type `data_type: varchar`
the length will not be checked.

### Schema check roadmap features

* Allow for optional columns
* Allow for extra columns not in the list
