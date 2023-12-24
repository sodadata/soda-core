# Soda Data Contracts

A data contract is a YAML file that describes the data in a dataset.

A dataset is a table or other tabular datastructure accessible via a SQL connection.

Soda can verify if the structure (= schema) and the data itself complies with the 
descriptions in the data contract file.

# Example

```yaml
dataset: DIM_CUSTOMER

columns:
    
  - name: id
    data_type: character varying
    checks:
      - type: no_missig_values
      - type: unique
    
  - name: cst_size_txt
    checks:
      - type: no_invalid_values
        valid_values: [1, 2, 3]
    
  - name: distance
    data_type: integer
    checks: 
      - type: avg
        fail_when_not_between: [50, 150]
          
  - name: country
    data_type: varchar
    checks:
      - type: no_missing_values
      - type: reference
        dataset: COUNTRIES
        column: id

checks:
  - type: row_count
    fail_when_not_between: [100, 500]
  - type: freshness_in_hours
    fail_when_greater_than: 6
```

# Contract basics

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

See [more dataset check examples](EXAMPLES.md#dataset-check-examples)

# Verifying a data contract

Verifying a contract means checking that data in a dataset complies with the information in the contract. This is 
also known as 'enforcement'.

Ideally you want to verify all new data before making it available to consumers in a table.  Therefore, it is best 
practice to store batches of new data first in a temporary table, verify the contract on there and only when that 
succeeds, append it to the larger table.

> Known limitation: At the moment there possibility to verify contracts using the CLI. Only a
> Python programmatic API is available.

In your python (virtual) environment, ensure that the libraries `soda-core` and `soda-core-contracts` are available
as well as the `soda-core-xxxx` library for the SQL engine of your choice.

To verify if a dataset complies with the contract, here's the code snippet.

Inputs: 
* Soda data contract YAML file
* Data source name
* Soda environment file containing data source configurations (=connections to SQL engines)

Output:
* Presence or absence of errors and warnings

```python
from soda.contracts.contract import Contract

connection_cfg_dict: Dict[str, str] = {
    "type": "postgres",
    "host": "localhost", 
    "username": "johndoe",
    "password": "*secret*"
}

with Connection.create_from_cfg_dict(connection_cfg_dict) as connection:
    
    contract = Contract.create_from_file(file_path='./customers.sdc.yml')
    contract.assert_no_errors()

    contract_verification_result = contract.verify(connection=connection, schema='TEST')
    contract_verification_result.assert_no_errors()
    
    
    if contract_verification_result.has_check_failures() or contract_verification_result.has_check_warnings():
      # Make the orchestration job fail.
      ...
    else:
      # Copy temporary table to the incremental table
      ...
```

# Schema for editing data contract YAML files

YAML editors can be configured with a JSON Schema to help with authoring data contract files.

[Soda data contract schema v1.0.0](./soda/contracts/soda_data_contract_schema_1_0_0.json)

## Applying the schema in VSCode

Ensure you have the YAML extension installed.

Download [the schema file](./soda/contracts/soda_data_contract_schema_1_0_0.json), put it somewhere relative to the contract YAML file and 
add `# yaml-language-server: $schema=./soda_data_contract_schema_1_0_0.json` on top of the contract YAML file like this: 

```yaml
# yaml-language-server: $schema=./contract_schema.json

dataset: CUSTOMERS

columns: 
    - ...
```

## Applying the schema in PyCharm

Download [the schema file](./soda/contracts/soda_data_contract_schema_1_0_0.json), put it somewhere in your project or on yourr file system. 

Go to: [Preferences | Languages & Frameworks | Schemas and DTDs | JSON Schema Mappings](jetbrains://Python/settings?name=Languages+%26+Frameworks--Schemas+and+DTDs--JSON+Schema+Mappings)

And add a mapping between *.sdc.yml files and the schema 
