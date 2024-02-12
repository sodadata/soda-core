# Implementing data contracts

Data Contracts are the best tool to prevent data issues from occurring. Data contracts
drives data producers to take ownership of the contract and the data.  A contract contains
information like the owner, schema, arrival times, data quality checks and is used for the
following use cases.

1) Source of information for data discovery tools
2) Verify the contract on new data and alert the producer if not compliant
3) Verify the contract on pipeline code changes and alert the producer if not compliant

Use contracts if you want to prevent data issues. It's a tool used by the producer, which
are the technical engineering team responsible for the pipeline component producing the data.

Data contracts are a solution used for applying encapsulation in the software engineering sense to 
data pipeline components. A data contract specifies the interface of the software component producing 
the data.  The interface is the dataset that can be queried with SQL.  It can be used to divide and 
conquer the big monolithic data pipelines into manageable pipeline components.

Contracts will significantly reduce the number of data issues that occur and it will
help the diagnostic process.

## 1) Writing contract YAML files

> Prerequisites: To write contract YAML files, you will need:
> * A YAML text editor
> * Optional: a local development environment to test contract YAML files
> * Optional: a git repository to version control your contract YAML file

See chapter [writing contract  YAML files](01_00_writing_contract_files.md) on how to write schema and
other data quality checks in Soda contract YAML files

## 2) Choose a way to verify a contract

> Prerequisites.  To verify a contract, you need:
> * A Soda contract YAML file
> * All connection details to your SQL engine like Snowflake or postgres

Choose the most way for you to verify your contract. The solutions to verifying a contract
below can be used to build your workflow for:
* Verifying a contract in production (when new data arrives)
* Verifying a contract in CI/CD (when pipeline code changes)
* Verifying a contract on a fixed time schedule (not recommended)

To make a choice, consider what triggers execution of your contract verification and the in what
environment will it run? For example a shell script in your local development environment using the Soda
contract API in a Python virtual environment, an Airflow operator in a DAG that is executed, a
GitHub commit, etc?

* [Verifying a contract in Python](02_verifying_a_contract_in_python/README.md):
  This is the most basic, common and versatile way to verify a contract.  In fact, all other ways below
  are based on this Python library approach.  To use our Soda Contracts Python library and API you
  have to know Python like setting up a Python virtual environment and Python library dependencies.
  Instructions are provided.

* [Verifying a contract using docker (Roadmap)](03_verifying_a_contract_using_docker/README.md):
  (Roadmap) If you know how to work with docker containers, then the Soda contract docker container removes the
  hassle of setting up the virtual environment and ensuring that the versions of all the libraries are
  compatible.

## 3) Configuring a connection

For each type of contract verification, you will need to configure a connection.

See [Configuring a connection](02_03_configuring_a_connection)

## 4) Enable contract check contributions

Once you have your development environment, production environment and potentially other staging
environments set up, consider to enable contributions to the contract by anyone in the organization.
People outside the producer team often have more domain knowledge about the data than the engineers
building the pipeline.  Soda offers a self-service user interface for anyone in the business to
contribute checks that can be proposed to the producers to be incorporated in the contract.

See [Enabling contract requests in Soda Cloud UI](07_enabling_contract_requests/README.md)
