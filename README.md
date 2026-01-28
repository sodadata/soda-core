
<h1 align="center">Soda Core — Data Contracts Engine</h1>

<p align="center">
  <a href="https://soda-community.slack.com/join/shared_invite/zt-3epazj3kw-00z15nnW4KEt4j_vk8lbdQ"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="#"><img src="https://static.pepy.tech/personalized-badge/soda-core?period=total&units=international_system&left_color=black&right_color=green&left_text=Downloads"></a>
</p>


Soda Core is a data quality and data contract verification engine. It lets you define data quality contracts in YAML and automatically validate both schema and data across your data stack.

Soda Core provides the Soda Command-Line Interface (CLI) and Python API, which you can use to generate, test, publish, and verify contracts. These operations can be executed locally during development, embedded programmatically within your data pipelines (Airflow, Dagster, Prefect, etc.), or executed remotely when connected to Soda Cloud.

## Highlights

- Define data contracts using a clean, human-readable YAML syntax
- Run checks on PostgreSQL, Snowflake, BigQuery, Databricks, DuckDB, and more
- Use 50+ built-in data quality checks for common and advanced validations
- Integrate with [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda_v4_release&utm_content=soda_cloud) for centralized management and anomaly detection monitoring

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
    
    
Soda Core v4 open source packages are available on public PyPI and have the form `soda-{data source}`:

```
pip install soda-postgres  # install latest version 4 package
```

Replace `soda-postgres` with the appropriate package for your data source.  For a list of supported data sources, packages, and configurations, see the [data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core).

### Working with legacy Soda Core v3

Soda package names have changed with the release of version 4.  Legacy version 3 open source packages have the form `soda-core-{data source}`.  For example, to install Soda Core v3 for Postgres, pinning the version at `3.5.x`:

```
pip install soda-core-postgres~=3.5.0   # install legacy version 3 package
```

For a list of supported data sources and other details, see the [v3 documentation within this repository](https://github.com/sodadata/soda-core/blob/v3/docs/installation.md).  For information about Soda Core v3, see the [v3 README file](https://github.com/sodadata/soda-core/blob/v3/README.md) and the [Soda v3 online documentation](https://docs.soda.io/soda-v3).  

## Quickstart

The examples show minimal configurations.  For more detailed examples and explanations, see the [Soda Cloud documentation](https://docs.soda.io/soda-v4/reference/cli-reference).

To see detailed logs, add  `--verbose` or `-v` to any of these commands.

### Configure a data source

To define a local configuration for your data source and validate the connection, run the following commands.

#### Create data source config

```
soda data-source create -f ds_config.yml
```

Parameter | Required | Description
--- | --- | ---
`-f`, `--file`|  Yes | Output file path for the data source YAML configuration file.


By default, the YAML file generated as `ds_config.yml` is a template for PostgreSQL connections.  To learn how to populate a data source configuration file, see the [data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core).

#### Test data source config

```
soda data-source test -ds ds_config.yml
```
Parameter | Required | Description
--- | --- | ---
`-ds`, `--data-source`| Yes | Path to a data source YAML configuration file.

### Create a contract

Create and populate new contract YAML file.  To understand how to write a contract, see the [online documentation](https://docs.soda.io/soda-v4/reference/contract-language-reference), or the example below, which is configured to test a table or view with qualified name `db.schema.dataset` within a data source named `postgres_ds`.  This table is assumed to have columns named `id`, `name`, and `size`. The data source name `postgres_ds` must match the `name` property in the data source configuration file.  

```
# contract.yml 

dataset: postgres_ds/db/schema/dataset

checks: # dataset level checks
  - schema:
  - row_count: 

columns: # columns block
  - name: id
    checks: # column level checks 
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

### Verify a contract locally

To evaluate a contract, run a contract verification scan:

```
soda contract verify -ds ds_config.yml -c contract.yml
```

Parameter | Required | Description
--- | --- | ---
`-ds`,`--data-source`| Yes | Path to a data source YAML configuration file.
`-c`,`--contract`| Yes | Path to a data contract YAML configuration file.


## Interact with Soda Cloud

To execute commands remotely, connect Sore Core to [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda_v4_release&utm_content=soda_cloud).  To learn how configure Soda Cloud, see the documentation about [configuring data sources and datasets](https://docs.soda.io/soda-v4/onboard-datasets-on-soda-cloud) and [working with contracts](https://docs.soda.io/soda-v4/data-testing/cloud-managed-data-contracts/author-a-contract-in-soda-cloud).

> **Request a free Soda Cloud account**
> 
> Request a [free account](https://soda.io/request-free/?utm_source=github&utm_medium=readme&utm_campaign=soda_v4_release&utm_content=free-account) to evaluate Soda Cloud. You’ll get access for up to three datasets.


### Connect to Soda Cloud

To connect Soda Core to Soda Cloud, generate a Soda Cloud config file:

```
soda cloud create -f sc_config.yml
```
Parameter | Required | Description
--- | --- | ---
`-sc`,`--soda-cloud`| Yes | Output file path for the Soda Cloud YAML configuration file.

Obtain Soda Cloud credentials and add them to the Soda Cloud config file.  To generate credentials, follow [this procedure](https://docs.soda.io/soda-v4/reference/generate-api-keys).

To test the connection:

```
soda cloud test -sc sc_config.yml
```
Parameter | Required | Description
--- | --- | ---
`-sc`,`--soda-cloud`| Yes | Path to a Soda Cloud YAML configuration file.

### Publish to Soda Cloud

To publish a local contract to Soda Cloud as the source of truth:

```
soda contract publish -c contract.yml -sc sc_config.yml
```

Parameter | Required | Description
--- | --- | ---
`-c`,`--contract`| Yes | Path to a contract YAML file.
`-sc`,`--soda-cloud`| Yes | Path to Soda Cloud YAML configuration file.

To publish local contract verification results to Soda Cloud, add a Soda Cloud YAML configuration file and enable the `publish` flag:


```
soda contract verify -ds ds_config.yml -c contract.yml -sc sc_config.yml -p
```

Parameter | Required | Description
--- | --- | ---
`-ds`,`--data-source`| Yes | Path to a data source YAML configuration file.
`-c`,`--contract`| Yes | Path to a data contract YAML configuration file.
`-sc`,`--soda-cloud`| Yes | Path to a Soda Cloud YAML configuration file.
`-p`,`--publish`| No | Publish results and contract to Soda Cloud. Requires "Manage contract" permission; [learn about permissions here](https://docs.soda.io/soda-v4/dataset-attributes-and-responsibilities).


### Verify a contract remotely using Soda Agent

To verify contracts via Soda Cloud using the [Soda Agent](https://docs.soda.io/soda-v4/reference/soda-agent-basic-concepts),  configure a dataset and contract in Soda Cloud and configure an agent in your environment.  To obtain the Soda Cloud dataset identifier, for example `postgres_ds/db/schema/dataset`, open the contract in Soda Cloud, enable the `Toggle Code` control, and copy the identifier from the first line of the contract.  To launch contract verification:

```
soda contract verify -sc sc_config.yml -d postgres_ds/db/schema/dataset -a 
```
Parameter | Required | Description
--- | --- | ---
`-a`,`--use-agent`| Yes | Use Soda Agent for execution
`-sc`,`--soda-cloud`| with `-a` | Path to a Soda Cloud YAML configuration file
`-d`,`--dataset`| with `-a` | Soda Cloud dataset identifier
`-p`,`--publish`| No | Publish results and contract to Soda Cloud. Requires "Manage contract" permission; [learn about permissions here](https://docs.soda.io/soda-v4/dataset-attributes-and-responsibilities).


To view more examples of interacting with Soda Cloud using Soda Core, see the [Soda documentation](https://docs.soda.io/soda-v4/reference/cli-reference).
