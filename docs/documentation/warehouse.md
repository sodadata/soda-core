---
layout: default
title: Warehouse YAML
parent: Documentation
nav_order: 3
---

# Warehouse YAML

A **warehouse** represents a SQL engine or database such as Snowflake, AWS Redshift, or PostgreSQL. You use a **warehouse YAML** file to configure connection details for Soda SQL to access your warehouse. 

## Create a warehouse YAML file

You need to create a **warehouse YAML** file for every warehouse to which you want to connect. You can create warehouse YAML files manually, but the CLI command `soda create` automatically prepares a warehouse YAML file and an env_vars YAML file for you. (Use the env-vars YAML to securely store warehouse login credentials. See [Env_vars YAML](#env_vars-yaml) below.)

When you execute the `soda create` command, you include options that instruct Soda SQL in the creation of the file, and you indicate the type of warehouse, a specification Soda SQL requires. Use `soda create --help` for a list of all available options. 

The example below provides the following details:
* option `-d` provides the name of the database
* option `-u` provides the username to log in to the database
* option `-w` provides the name of the warehouse
* requirement `postgres` indicates the type of database 


Command:
```shell
$ soda create -d sodasql -u sodasql -w soda_sql_tutorial postgres
```
Output:
```shell
  | Soda CLI version 2.x.xx
  | Creating warehouse YAML file warehouse.yml ...
  | Creating /Users/Me/.soda/env_vars.yml with example env vars in section soda_sql_tutorial
  | Review warehouse.yml by running command
  |   cat warehouse.yml
  | Review section soda_sql_tutorial in ~/.soda/env_vars.yml by running command
  |   cat ~/.soda/env_vars.yml
  | Then run the soda analyze command
```

In the above example, Soda SQL created a warehouse YAML file and put it in a **warehouse directory** which is the top directory in your Soda SQL project directory structure. (It put the env_vars YAML file in your local user home directory.)


## Anatomy of the warehouse YAML file

When it creates your warehouse YAML file, Soda SQL pre-populates it with the options details you provided. The following is an example of a warehouse YAML file that Soda SQL created and pre-populated.

```yaml
name: soda_sql_tutorial
connection:
  type: postgres
  host: localhost
  username: env_var(POSTGRES_USERNAME)
  password: env_var(POSTGRES_PASSWORD)
  database: sodasql
  schema: public
```

Notice that even though the command provided a value for `username`, Soda SQL automatically used `env_var(POSTGRES_USERNAME)` instead. By default, Soda SQL stores database login credentials in an env_vars YAML file so that this sensitive information stays locally stored. See [Env_vars YAML](#env_vars-yaml) below for details.

Each type of warehouse requires different configuration parameters. Refer to [Set warehouse configurations]({% link documentation/warehouse_types.md %}) for details that correspond to the type of database you are using. 


# Env_vars YAML

To keep your warehouse YAML file free of login credentials, Soda SQL references environment variables. When it creates a new warehouse YAML file, Soda SQL also creates an **env_vars YAML** file to store your database username and password values. Soda SQL does not overwrite or remove and existing environment variables, it only adds new. 

When it [runs a scan]({% link documentation/scan.md %}#run-a-scan), Soda SQL loads environment variables from your local user home directory where it stored your env_vars YAML file. 

Use the command `cat ~/.soda/env_vars.yml` to review the contents of your env_vars YAML file. Open the file from your local user home directory to input the values for your database credentials.

```yaml
soda_sql_tutorial:
    POSTGRES_USERNAME: myexampleusername
    POSTGRES_PASSWORD: myexamplepassword

some_other_soda_project:
    SNOWFLAKE_USERNAME: someotherexampleusername
    SNOWFLAKE_PASSWORD: someotherexamplepassword
```

## Go further

* Set the [warehouse configuration parameters]({% link documentation/warehouse_types.md %}) for your type of database.
* Learn more about [How Soda SQL works]({% link documentation/concepts.md %}).
* Learn more about the [scan YAML]({% link documentation/scan.md %}) file.
