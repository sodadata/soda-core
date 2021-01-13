# Warehouse

A directory with a `warehouse.yml` configuration file is considered a Soda
warehouse directory. It implies a directory structure where each table in your
warehouse is represented by a subdirectory containing a `scan.yml` file.

A warehouse represents a connection to any SQL engine like: Snowflake, Redshift,
BigQuery, Athena, PostgreSQL, etc.

## Example

Here's an example of a Soda warehouse directory structure:

```
+ sales_snowflake
|   + warehouse.yml
|   + customers
|   |   + scan.yml
|   + invoices
|   |   + scan.yml
|   |   + invoices_without_active_country.yml
|   |   + invoices_with_inactive_products.yml
```

`warehouse.yml` contains the name of the warehouse and
the connection details (see below).

`customers` and `invoices` are table directories, each
having a [scan.yml](scan.md) configuration file.

`invoices_*.yml` are custom user defined [SQL metrics](sql_metrics.md)
which get executed when a table scan is performed.

## warehouse.yml

`warehouse.yml` contains the name of the warehouse and the connection details.

We encourage usage of environment variables for credentials and other sensitive
information to prevent them from being checked-in into your Version Control System.

An example of a configuration connecting to a PostgreSQL database may look like:
```yaml
name: my_project_postgres
warehouse:
  type: postgres
  host: localhost
  username: env_var(POSTGRES_USERNAME)
  password: env_var(POSTGRES_PASSWORD)
  database: sodasql
  schema: public
```

> Each environment variable, defined using `env_var(VAR_NAME)`, will automatically be
resolved using the `env_vars.yml` file in your home directory. More on this can be found
in the [cli.yml documentation](cli.md#env-vars.md).

Soda SQL supports many different warehouses. Each of them require different configuration parameters.
See [Warehouse types](warehouse_types.md) to learn how to configure each
type of warehouse.

Soon, Soda warehouse files will allow you to set-up a link with your Soda Cloud account.
A Soda Cloud account enables you to push the metric and test results after each scan and
share them with other people in your data organisation.
