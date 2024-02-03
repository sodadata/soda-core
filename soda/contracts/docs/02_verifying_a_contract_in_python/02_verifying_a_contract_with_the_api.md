# Contract API

This section explains the Python API to verify a contract.

The contract object is created from a YAML file (or string).

Contract verification performed executed on a SQL connection to a warehouse.


## Requires

* A virtual environment with `soda-core-contracts` installed.
  See [Setting up a soda contracts virtual environment](./01_setting_up_a_python_virtual_environment_for_soda_contracts)

## The API

First we start with a complete example that you can copy-and-paste.  Then we explain the sections in there one by one.

```python
from soda.contracts.connection import Connection
from soda.contracts.contract import Contract, ContractResult

try:
    with Connection.from_yaml_file("./postgres_localhost_dev.scn.yml") as connection:
        contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
        contract_result: ContractResult = contract.verify(connection)
except SodaException as e:
    # make the orchestration job fail and report all problems
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

> Known limitation: At the moment there is no possibility to verify contracts using the CLI. Only a
> Python programmatic API is available.

In your python (virtual) environment, ensure that the libraries `soda-core` and `soda-core-contracts` are available
as well as the `soda-core-xxxx` library for the SQL engine of your choice.

To verify a contract, you need a [connection](#creating-a-connection) and a contract file:

```python
contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
contract_result: ContractResult = contract.verify(connection, soda_cloud)
```

## Errors, failures and problems

**Errors** are logs with level error that indicate the checks in the contract could not be evaluated. Contract verification
produces logs. Logs have a level of debug, info, warning and error. This could be due to incorrect inputs like missing files,
invalid files, connection issues, or contract invalid format, or it could be due to query execution exceptions.

**Check failures** are failed checks that have evaluated as part of a contract. A check failure implies that the data is
not matching the description in the contract in one way or another.

A **problem** is the common term for either an execution error or a failure.  Contract verification should have no errors nor
check failures. You will find convenience methods related to problems on the ContractResult like has_problems and
assert_no_problems.

## Exceptions vs logs

Errors may lead to exceptions being raised but that is not always the
case. Connection errors and file parsing errors typically lead to exceptions. In that case fail fast and hard is appropriate as
these are typically unrecoverable. > On the other hand, the contract verification is as resilient as possible and all errors and
failures are handled & collected as logs. The reason is that we want to compute as many check results as possible to build up
history and report on as many problems as possible in one verification.  More on this in
[this ADR on exceptions vs errors](../adr/03_exceptions_vs_error_logs)

We do not comply with the standard Python language convention of errors for invalid inputs and exceptions
for execution issues. The base class for all Soda exceptions is SodaException, which inherits from Exception.

The API aims always to catch any other exceptions and only allow for SodaException's to come out of the
API methods

### Contract YAML files

You can use variables to get contracts variations. Use syntax `${SCHEMA}` for resolving environment variables in a contract YAML file.
Variable resolving is case sensitive. We recommend using all upper case and underscores for naming environment variables.

For contract YAML files, we recommend to use extension `sdc.yml` (Soda Data Contract) because
the editor tooling support will likely will be coupled via that extension.

[Learn more on writing contract checks](./020_writing_a_contract)


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

> Next, learn more about [Writing contracts](../01_writing_contract_yaml_files/README.md)
