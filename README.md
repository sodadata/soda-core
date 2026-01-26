
<h1 align="center">Soda Core</h1>

TODO re-add preamble


## Installation

This repository hosts the open source Soda Core packages which are installable using the **Public PyPI installation flow** described in [Soda's documentation](https://docs.soda.io/soda-v4/deployment-options/soda-python-libraries#public-pypi-installation-flow). 

### Requirements
To use Soda, you must have installed the following on your system.

* **Python 3.8, 3.9, 3.10 or 3.11.** <br>
To check your existing version, use the CLI command: `python --version` or `python3 --version`. If you have not already installed Python, consider using `pyenv` to manage multiple versions of Python in your environment.

* **Pip 21.0 or greater.**
To check your existing version, use the CLI command: `pip --version`

* Optionally, **a Soda Cloud account**; see [how to sign up](https://docs.soda.io/soda-v4/quickstart#sign-up).

Best practice dictates that you install the Soda CLI using a virtual environment. If you haven't yet, in your command-line interface tool, create a virtual environment in the `.venv` directory using the commands below. Depending on your version of Python, you may need to replace `python` with `python3` in the first command.

```
python -m venv .venv
source .venv/bin/activate
```

## Get started
    To use the open source Soda Core python packages, you may install them from the public Soda PyPi registry: https://pypi.cloud.soda.io/simple .

Install the Soda Core package for your data source. This gives you access to all the basic CLI functionality for working with contracts.

```
pip install -i https://pypi.cloud.soda.io/simple --pre -U "soda-postgres>4"
```
Replace `soda-postgres` with the appropriate package for your data source. See the [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core) for supported packages and configurations.



## Quickstart

The examples show a minimal configuration of a data source and contract.  Please see the Soda Cloud documentation for more detailed examples as well as features available for Soda Cloud users.  (TODO link)

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

To see how to modify it to connect to other data sources, head to [Data source reference for Soda Core](link todo).

#### Test data source connection

```
soda data-source test -ds ds_config.yml
```
Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--verbose`, `-v` | No | Display detailed logs during execution.

### Create a contract

TODO this should be a barebones example without any Soda Cloud features

### Verify a contract

Executes a contract verification to check if the dataset complies with its expectations. You can verify a local contract file or a Soda Cloud contract either locally (in your Python environment) or remotely with a Soda Agent.


```
soda contract verify --data-source ds_config.yml --contract contract.yaml
```


Parameter | Required | Description
--- | --- | ---
`--data-source`, `-ds` | Yes | Path to a data source YAML configuration file.
`--contract`, `-c` | Yes | Path to a data source YAML configuration file.
`--verbose`, `-v` | No | Display detailed logs during execution.
