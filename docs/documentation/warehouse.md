---
layout: default
title: Warehouse
parent: Documentation
nav_order: 3
---

# Warehouse

A directory with a `warehouse.yml` configuration file is considered a Soda
warehouse directory. By default Soda SQL places its Scan YAML files in a
subdirectory called `./tables`, although this isn't enforce nor required. Feel
free to structure your project anyway you like.

A warehouse represents a connection to any SQL engine like: `Snowflake`, `Redshift`,
`BigQuery`, `Athena`, `PostgreSQL`, etc.

### Example

Here's an example of a Soda Warehouse directory structure which is generated
when using `soda init` to setup your project:

```
+ sales_snowflake
|   + warehouse.yml
|   + tables
|   |   + scan.yml
|   |   + orders.yml
```

##### `warehouse.yml`
This file contains the name of your warehouse and its
connection details (see below)

##### `./tables`
This directory contains all of your [Scan YAML]({% link documentation/scan.md %}) configuration files.

## warehouse.yml

This contains the name of the warehouse and the connection details.

We encourage usage of environment variables for credentials and other sensitive information
to prevent them from being checked-in into your version control system.

For example:
```yaml
name: my_project_postgres
connection:
  type: postgres
  host: localhost
  username: env_var(POSTGRES_USERNAME)
  password: env_var(POSTGRES_PASSWORD)
  database: sodasql
  schema: public
```

The example above shows you how to use environment variables for your credentials.
Each environment variable, defined using `env_var(VAR_NAME)`, will automatically be
resolved using the `env_vars.yml` file in your home directory. More on this can be found
in the [cli.yml documentation]({% link documentation/cli.md %}#env-vars).

Each warehouse will require different configuration parameters.
See [Warehouse types]({% link documentation/warehouse_types.md %}) to learn how to configure each
type of warehouse.

> Soon, Soda project files will also include an optional
link to a Soda cloud account.  A cloud account enables you to push the monitoring
results after each scan and share them with other people in your data organisation.