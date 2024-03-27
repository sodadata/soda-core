# Verifying a contract

* [Contract verification environments](#contract-verification-environments)
* [Verifying a contract with the Python API](#verifying-a-contract-with-the-python-api)
  * [Setting up a Soda contracts virtual environment](#setting-up-a-soda-contracts-virtual-environment)
  * [Contract verification Python snippet](#contract-verification-python-snippet)
* [Creating a connection](#creating-a-connection)
  * [Creating a postgres connection](#creating-a-postgres-connection)
* [The circuit breaker pattern](#the-circuit-breaker-pattern)


### Contract verification environments

Verifying a contract means to verify if (new) data in a dataset complies a contract descriptions.
It's typically done at the end of a data pipeline right after new data was produced.  The schema will
be verified along with all data quality properties specified in the contract. This section lists the
different environments and approaches where contract verification is appropriate.

A contract is commonly verified after new data has been produced. Often data production is coordinated
through an orchestration tool like for example Airflow. The benefit of this approach is that there is that
there is no time delay between new data becoming available and the data verification.

Even when testing the new, incremental data right after it is produced, it still may be that the
data is bad and already exposed to consumers.  To avoid this, consider the
[circuit breaker pattern](#the-circuit-breaker-pattern).

In CI/CD, contract verification is executed because pipeline code changes may have caused changes to the
produced data.  So when creating a PR, it makes sense to verify the contract to see if any unwanted changes
to the data have creeped in.

On a developers local environment, a contract can be verified as part of the development of a new
feature.

Data contract verification can also be executed on a time based schedule.  This is often inferior as
it leaves a period of time where data is produced and not verified.  It's always preferrable to verify
data as soon as it is made available.

### Verifying a contract with the Python API

> Prerequisites.  To verify a contract, you need:
> * A Soda contract YAML file
> * All connection details to your SQL engine like Snowflake or postgres
> * Python 3.8 or higher

This section explains the Python API to verify a contract.  The contract object is created from
a YAML file (or string). Contract verification performed executed on a SQL connection to a
warehouse.

The contract Python library is the most basic, common and versatile way to verify a contract.
It's the lowest level component that can be used in all the environments.  We may have more specific
support for environments like Airflow or Docker.  But if not, you can use the Python API to
embed contract verification into your environment.

### Setting up a Soda contracts virtual environment

> Alternative: As an alternative to setting up your own virtual environment, consider using
> a Soda docker container.  That will have a tested combination of all the library dependencies.

> Prerequisites:
> * Linux or Mac
> * You have installed Python 3.8 or greater.
> * You have installed Pip 21.0 or greater.

This tutorial references a MacOS environment.

```shell
mkdir soda_contract
cd soda_contract
# Best practice dictates that you install the Soda using a virtual environment. In your command-line interface, create a virtual environment in the .venv directory, then activate the environment.
python3 -m venv .venv
source .venv/bin/activate
# Execute the following command to install the Soda package for PostgreSQL in your virtual environment. The example data is in a PostgreSQL data source, but there are 15+ data sources with which you can connect your own data beyond this tutorial.
pip install -i https://pypi.cloud.soda.io soda-postgres
pip install -i https://pypi.cloud.soda.io soda-core-contracts
# Validate the installation.
soda --help
```

To exit the virtual environment when you are done with this tutorial, use the command deactivate.

### Contract verification Python snippet

First we start with a complete example that you can copy-and-paste.  Then we explain the sections in there one by one.

Verifying a contract means checking that data in a dataset complies with the information in the contract. This is
also known as 'enforcement'.

Ideally you want to verify all new data before making it available to consumers in a table.  Therefore, it is best
practice to store batches of new data first in a temporary table, verify the contract on there and only when that
succeeds, append it to the larger table.

> Known limitation: At the moment there is no possibility to verify contracts using the CLI. Only a
> Python programmatic API is available.

In your python (virtual) environment, ensure that the libraries `soda-core` and `soda-core-contracts` are available
as well as the `soda-core-xxxx` library for the SQL engine of your choice.

```python
from soda.contracts.data_source import Connection
from soda.contracts.contract import Contract, ContractResult

try:
    with Connection.from_yaml_file("./postgres_localhost_dev.scn.yml") as connection:
        contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
        contract_result: ContractResult = contract.verify(connection)
except SodaException as e:
# make the orchestration job fail and report all problems
```

### Creating a connection

A contract is verified over a connection to a SQL engine. Internally the implementation is based on
a Python DBAPI connection.

A connection can be created based on a YAML connection configuration file, a YAML string or a dict.
Use one of the `Connection.from_*` methods to create a connection.

It's good practice to [use environment variables](#environment-variables-and-defaults) in your YAML
files to protect your credentials. Use syntax `${POSTGRES_PASSWORD}` for resolving environment variables.
Variable resolving is case sensitive. We recommend using all upper case and underscores for naming
environment variables.

For Soda connection configuration YAML files, we recommend to use extension `connection.yml`. because
the editor tooling support will likely will be coupled via that extension.

#### Creating a `postgres` connection

To create a postgres connection with a username/password:
```yaml
type: postgres
host: localhost
database: yourdatabase
username: ${POSTGRES_USERNAME}
password: ${POSTGRES_PASSWORD}
```

> TODO: show examples of all other types of postgres connection configurations.

## Soda Cloud configuration properties

What is it?  Where to get it?  What's the default value?

host (default cloud.soda.io)
api_key_id (required)
api_key_secret (required)
scheme (default https)

## Environment variables and defaults

You can use variables to get contracts variations. Use syntax `${SCHEMA}` for resolving environment variables in a contract YAML file.
Variable resolving is case sensitive. We recommend using all upper case and underscores for naming environment variables.

Precedence of values
* lower case env vars
* upper case env vars
* provided configuration value
* default value

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

## The circuit breaker pattern

TODO
