
<h1 align="center">Soda Core — Data Contracts Engine</h1>

Soda Core is a data quality and data contract verification engine. It lets you define data quality contracts in YAML and automatically validate both schema and data across your data stack.

Soda Core provides the Soda Command-Line Interface (CLI), which you can use to generate, test, publish, and verify contracts. These operations can be executed locally during development or remotely when connected to Soda Cloud.

## Highlights

- Define data contracts using a clean, human-readable YAML syntax
- Run checks on PostgreSQL, Snowflake, BigQuery, Databricks, DuckDB, and more
- Use 50+ built-in data quality checks for common and advanced validations
- Integrate with [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) for centralized management and anomaly detection monitoring

## Setup

This repository hosts the open source Soda Core packages which are installable using the **Public PyPI installation flow** described in [Soda's documentation](https://docs.soda.io/soda-v4/deployment-options/soda-python-libraries#public-pypi-installation-flow). 

### Requirements
To use Soda, you must have installed the following on your system.

* **Python 3.9, 3.10, 3.11, or 3.12** <br>
To check your existing version, use the CLI command: `python --version` or `python3 --version`. If you have not already installed Python, consider using `pyenv` to manage multiple versions of Python in your environment.  **Note:** While Python 3.12 is the highest officially supported version, there are no known issues preventing use of Python 3.13+.

* **Pip 21.0 or greater.**
To check your existing version, use the CLI command: `pip --version`

We recommend that you install Soda Core using `uv` or a virtual environment. 

### Installation
    
    
To use the open source Soda Core python packages, you may install them from the public Soda PyPi registry: https://pypi.cloud.soda.io.

```
pip install -i https://pypi.cloud.soda.io --pre -U "soda-postgres>4"
```
Replace `soda-postgres` with the appropriate package for your data source. See the [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core) for supported packages and configurations.



## Quickstart

The examples show a minimal configuration of a data source and contract.  Please see the [Soda Cloud documentation](https://docs.soda.io/soda-v4/reference/cli-reference) for more detailed examples as well as features available for [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) users.  

Most commands can be run with `--verbose` or `-v` to display detailed logs during execution.

### Configure a data source
These commands help you define a local configuration for your data source and validate the connection.

#### Create data source config

```
soda data-source create -f ds_config.yml
```

Parameter | Required | Description
--- | --- | ---
`--file`, `-f` |  Yes | Output file path for the data source YAML configuration file.


By default, the YAML file generated as `ds_config.yml` is a template for PostgreSQL connections.

To see how to modify it to connect to other data sources, head to [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core).

#### Test data source config

```
soda data-source test -ds ds_config.yml
```
Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.

### Create a contract

Create a new file named `contract.yml`.  The following sample contract will run against a table with qualified name `db.schema.dataset` within a data source named `postgres_ds`.  This data source name must match the name in the data source config file.  This table is assumed to have columns named `id`, `name`, and `size`.

```
dataset: postgres_ds/db/schema/dataset

checks: # dataset level checks
  - schema:
  - row_count: 

columns: # columns block
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

Please view the Soda documentation for a [full reference of contracts and check definitions](https://docs.soda.io/soda-v4/reference/contract-language-reference).  


### Verify a contract locally

You may run a contract verification scan to evaluate a dataset with respect to a contract, as follows:

```
soda contract verify -ds ds_config.yml -c contract.yml
```

Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--contract`, `-c` | Yes | Path to a data contract YAML configuration file.
`--publish, -p` | No | Publish results and contract to Soda Cloud.


## Interact with Soda Cloud

Sode Core also allows you to connect to [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=soda_cloud) and perform operations remotely instead of locally.   Please see the documentation for examples on [configuring data sources and datasets](https://docs.soda.io/soda-v4/onboard-datasets-on-soda-cloud) and [working with contracts](https://docs.soda.io/soda-v4/data-testing/cloud-managed-data-contracts/author-a-contract-in-soda-cloud) in Soda Cloud.

> **Request a free Soda Cloud account**
> 
> Request a [free account](https://soda.io/request-free/?utm_source=github&utm_medium=readme&utm_campaign=soda-core&utm_content=free-account) to evaluate Soda Cloud. You’ll get access for up to three datasets.


### Connect to Soda Cloud

Generate a Soda Cloud config file named `sc_config.yml`:

```
soda cloud create -f sc_config.yml
```

Follow these instructions to [generate API keys](https://docs.soda.io/soda-v4/reference/generate-api-keys), and then add them the Soda Cloud config file.  You can test the connection as follows:

```
soda cloud test -sc sc_config.yml
```
Parameter | Required | Description
--- | --- | ---
`--soda-cloud, -sc`, `-f`| Yes | Path to a Soda Cloud YAML configuration file.

### Publish to Soda Cloud

You may publish a local contract to Soda Cloud, which makes it the source of truth for verification.

```
soda contract publish -c contract.yaml -sc sc_config.yml
```

Parameter | Required | Description
`--contract`, `-c`  | Yes | Path to a contract YAML file.
`--soda-cloud`, `-sc` | Yes | Path to Soda Cloud YAML configuration file.

You may also publish local contract verification results to Soda Cloud by adding a Soda Cloud YAML configuration file and enabling the `publish` flag:


```
soda contract verify -ds ds_config.yml -c contract.yml -sc sc_config.yml -p
```

Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--contract`, `-c` | Yes | Path to a data contract YAML configuration file.
`--soda-cloud, -sc` | Yes | Path to a Soda Cloud YAML configuration file.
`--publish`, `-p` | No | Publish results and contract to Soda Cloud. Requires "Manage contract" permission; [learn about permissions here](https://docs.soda.io/soda-v4/dataset-attributes-and-responsibilities).


### Verify a contract remotely using Soda Agent

You may verify contracts via Soda Cloud using the [Soda Agent](https://docs.soda.io/soda-v4/reference/soda-agent-basic-concepts).   Once you have configured a dataset and contract, and assuming your Soda Cloud dataset identifier is `postgres_ds/db/schema/dataset`, launch contract verification as follows:

```
soda contract verify -sc soda_cloud.yml -d postgres_ds/db/schema/dataset -a 
```
Parameter | Required | Description
--- | --- | ---
`--use-agent`, `-a` | Yes | Use Soda Agent for execution
`--soda-cloud`, `-sc` | with `-a` | Path to a Soda Cloud YAML configuration file
`--dataset`, `-d` | with `-a` | Soda Cloud dataset identifier
`--publish`, `-p `| No | Publish results and contract to Soda Cloud. Requires "Manage contract" permission; [learn about permissions here](https://docs.soda.io/soda-v4/dataset-attributes-and-responsibilities).


Please see the [Soda documentation](https://docs.soda.io/soda-v4/reference/cli-reference) for more examples of interacting with Soda Cloud using Soda Core.
