# Schema check

### Basic schema check

Verify the schema by adding a schema check to the list of checks.
This is the most common schema check. The list of column must be exact as 
specified in the contract.  No extra nor missing columns are allowed.  Column 
names are case sensitive.  

If the data type is specified on the column, the schema check will also verify 
that they match. 

For example:

```
dataset: dim_employee
columns:
  - name: id
    data_type: character varying
  - name: last_name
    data_type: character varying
  - name: address_line1

checks:
  - type: schema
```

The above check will verify that the table `dim_employee` has exact 
3 columns named `id`, `last_name` and `address_line1`.  For columns `id`
and `last_name`, the warehouse data type has to match the provided, 
data-source-specific data types.

### Data type limitation

> Limitation: At the moment, the schema check data type check is based on 
> comparing the data type name coming from the warehouse metadata value 
> with the `data_type` value in the contract.  There is no support yet 
> for handling synonyms like varchar == character varying and to test the 
> length of varchar fields like varchar(255).  The name has to be exact 
> as in the metadata.

### Optional columns

Not implemented yet
