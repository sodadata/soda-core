
<h1 align="center">Soda Core — Data Contracts Engine</h1>

Soda Core is a data quality and data contract verification engine. It lets you define data quality contracts in YAML and automatically validate both schema and data across your data stack.

Soda Core runs contracts as part of your pipelines and orchestration tools, making data quality enforcement scalable, automated, and easy to integrate.

## Highlights

- Define data contracts using a clean, human-readable YAML syntax
- Run checks on PostgreSQL, Snowflake, BigQuery, Databricks, DuckDB, and more
- Use 50+ built-in data quality checks for common and advanced validations
- Integrate with [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) for centralized management and anomaly detection monitoring

## Setup

This repository hosts the open source Soda Core packages which are installable using the **Public PyPI installation flow** described in [Soda's documentation](https://docs.soda.io/soda-v4/deployment-options/soda-python-libraries#public-pypi-installation-flow). 

### Requirements
To use Soda, you must have installed the following on your system.

* **Python 3.8, 3.9, 3.10 or 3.11.** <br>
To check your existing version, use the CLI command: `python --version` or `python3 --version`. If you have not already installed Python, consider using `pyenv` to manage multiple versions of Python in your environment.

* **Pip 21.0 or greater.**
To check your existing version, use the CLI command: `pip --version`

We recommend that you install Soda Core using a virtual environment. 

### Installation
    To use the open source Soda Core python packages, you may install them from the public Soda PyPi registry: https://pypi.cloud.soda.io/simple .

Install the Soda Core package for your data source. This gives you access to all the basic CLI functionality for working with contracts.

```
pip install -i https://pypi.cloud.soda.io/simple --pre -U "soda-postgres>4"
```
Replace `soda-postgres` with the appropriate package for your data source. See the [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core) for supported packages and configurations.



## Quickstart

The examples show a minimal configuration of a data source and contract.  Please see the [Soda Cloud documentation](https://docs.soda.io/soda-v4/reference/cli-reference) for more detailed examples as well as features available for [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) users.  

### Configure a data source
These commands help you define a local configuration for your data source (used by Soda Core) and validate the connection.

#### Create data source config

```
soda data-source create -f ds_config.yml
```

Parameter | Required | Description
--- | --- | ---
`--file`, `-f` |  Yes | Output file path for the data source YAML configuration file.
`--verbose`, `-v` | No | Display detailed logs during execution.

By default, the YAML file generated as `ds_config.yml` is a template for PostgreSQL connections.

To see how to modify it to connect to other data sources, head to [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core).

#### Test data source connection

```
soda data-source test -ds ds_config.yml
```
Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--verbose`, `-v` | No | Display detailed logs during execution.

### Create a contract

Create a new file named `contract.yml`.  The following sample contract will run against a table with qualified name `db.schema.dataset` within a data source named `postgres_ds`.  The data source name must match the name in the data source config file.

```
dataset: postgres_ds/db/schema/dataset

checks: #dataset level checks
  - schema:
  - row_count: 

columns: #columns block
  - name: id
    checks: # column level checks (optional)
      - missing:
  - name: name
    checks:
      - missing:
          threshold:
            metric: percent
            must_be_less_than: 10
  - name: size
    checks:
      - invalid:
          valid_values: ['S', 'M', 'L'] 
```

For a full reference of contracts including available check definitions, please view the [Soda documentation](https://docs.soda.io/soda-v4/reference/contract-language-reference).  



### Verify a contract

Executes a contract verification to check if the dataset complies with its expectations. You can verify a local contract file or a [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) contract either locally (in your Python environment) or remotely with a Soda Agent.


```
soda contract verify --data-source ds_config.yml --contract contract.yml
```


Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--contract`, `-c` | Yes | Path to a data contract YAML configuration file.
`--verbose`, `-v` | No | Display detailed logs during execution.
