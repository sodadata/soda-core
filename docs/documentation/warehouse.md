---
layout: default
title: Warehouse YAML
parent: Documentation
nav_order: 3
---

# Warehouse YAML

A **warehouse** represents a SQL engine or database that contains data that you wish to test and monitor, such as Snowflake, AWS Reshift, or PostreSQL. You use a **warehouse YAML** file to configure warehouse connection details, one warehouse YAML per warehouse. 

## Create a warehouse YAML file

You need to create a **warehouse YAML** file for every warehouse to which you want to connect. You can create warehouse YAML files manually, but the CLI command `soda create` automatically prepares a warehouse YAML file and an env_vars YAML file for you. Use the env-vars YAML to securely store warehouse login credentials.

When it creates a warehouse YAML file, Soda SQL puts it in a **warehouse directory** which is the top directory in your Soda SQL directory structure. 


### Example

Here's an example of a Soda Warehouse directory structure which is generated
when using `soda analyze` to setup your project:

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
This directory contains all of your [scan YAML]({% link documentation/scan.md %}) configuration files.

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


# Env vars

To keep your `warehouse.yml` configuration files free of credentials, soda-sql
supports to reference to environment variables by using the `env_var(SOME_ENV_VAR)` format.

The `soda` CLI uses a convenient mechanism to load environment variables from your local
user home directory.  Each `soda` CLI command which reads a warehouse configuration will
also read the corresponding environment variables specified in your
`~/.soda/env_vars.yml` file.

Example `~/.soda/env_vars.yml`
```yaml
my_project_postgres:
    SNOWFLAKE_USERNAME: someotherexampleusername
    SNOWFLAKE_PASSWORD: someotherexamplepassword

some_other_soda_project:
    POSTGRES_USERNAME: myexampleusername
    POSTGRES_PASSWORD: myexamplepassword
```

The `soda create` command will assist in creating and prepopulating the
environment variables section in your `~/.soda/env_vars.yml` file.