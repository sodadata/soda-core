from soda.contracts.contract_result import ContractResult

# Contract API

## The API

```python
from soda.contracts.connection import Connection
from soda.contracts.contract import Contract
from soda.contracts.soda_cloud import SodaCloud

try:
    soda_cloud: SodaCloud = SodaCloud.from_yaml_file("./soda_cloud.scl.yml")
    with Connection.from_yaml_file("./postgres_localhost_dev.scn.yml") as connection:
        contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
        contract_result: ContractResult = contract.verify(connection, soda_cloud)
        contract_result.assert_no_errors()
        if contract_result.has_failures():
            # make the orchestration job fail and report contract check failures.
except SodaException as e:
    # make the orchestration job fail and report contract verification errors.
```

## Creating a connection

A contract is verified over a connection to a SQL engine. Internally the implementation is based on 
a Python DBAPI connection.  

A connection can be created based on a YAML connection configuration file, a YAML string or a dict. 
Use one of the `Connection.from_*` methods to create a connection.

```python
with Connection.from_yaml_file("./postgres.scn.yml") as connection:
    # Do stuff with connection
```

### Connection YAML configuration files

It's good practice to use environment variables in your YAML files to protect your credentials.  
Use syntax `${POSTGRES_PASSWORD}` for resolving environment variables.  Variable resolving is 
case sensitive. We recommend using all upper case and underscores for naming environment variables. 

For connection configuration YAML files, we recommend to use extension `scn.yml` (Soda ConnectioN) because 
the editor tooling support will likely will be coupled via that extension. 

### A postgres connection

To create a postgres connection with a username/password:
```yaml
type: postgres
host: localhost
database: yourdatabase
username: ${POSTGRES_USERNAME}
password: ${POSTGRES_PASSWORD}
```

> TODO: show examples of all other types of postgres connection configurations.

## Verifying a contract

Verifying a contract means checking that data in a dataset complies with the information in the contract. This is 
also known as 'enforcement'.

Ideally you want to verify all new data before making it available to consumers in a table.  Therefore, it is best 
practice to store batches of new data first in a temporary table, verify the contract on there and only when that 
succeeds, append it to the larger table.

> Known limitation: At the moment there possibility to verify contracts using the CLI. Only a
> Python programmatic API is available.

In your python (virtual) environment, ensure that the libraries `soda-core` and `soda-core-contracts` are available
as well as the `soda-core-xxxx` library for the SQL engine of your choice.

To verify a contract, you need a [connection](#creating-a-connection) and a contract file:

```python
contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
contract_result: ContractResult = contract.verify(connection, soda_cloud)
contract_result.assert_no_errors()
contract_result.assert_no_check_failures()
```

## Exceptions, errors and failures

Contract verification **errors** are problems during verification of the contract. These could be file format violations, 
missing files, queries that fail etc.

Contract check **failures** imply that the data is not matching the description in the contract in one way or another.

Connection errors generally lead to exceptions. Fail fast and hard as these are typically unrecoverable.

The contract verification however is as resilient as possible and all errors and failures are handled & collected. 
The reason is that we want to compute as many check results as possible to build up history and report on as many problems 
as possible in one verification. 

> Note: We do not comply with the standard Python language here wrt errors and exceptions. 

### Contract YAML files

You can use variables to get contracts variations. Use syntax `${SCHEMA}` for resolving environment variables in a contract YAML file.  
Variable resolving is case sensitive. We recommend using all upper case and underscores for naming environment variables. 

For contract YAML files, we recommend to use extension `sdc.yml` (Soda Data Contract) because 
the editor tooling support will likely will be coupled via that extension.

[Learn more on writing contract checks](./contract_checks.md)







----
TODO Review all below

----



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

    contract_result = contract.verify(connection=connection, schema='TEST')
    contract_result.assert_no_errors()
    
    
    if contract_result.has_check_failures() or contract_result.has_check_warnings():
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

## API to create a SodaCloud




### From YAML file or YAML string

### From env vars only

## Environment variables and defaults

Precedence of values 
* lower case env vars
* upper case env vars
* provided configuration value
* default value

## Soda Cloud configuration properties

What is it?  Where to get it?  What's the default value?

host (default cloud.soda.io)
api_key_id (required)
api_key_secret (required)
scheme (default https)
