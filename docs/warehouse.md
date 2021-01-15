# Warehouse

A directory with a `warehouse.yml` configuration file is considered a Soda
warehouse directory and it implies a directory structure of one directory
per table.

A warehouse represents a connection to any SQL engine like: Snowflake, Redshift,
BigQuery, Athena, Postgres, etc .

## Example

Here's an example Soda warehouse directory structure:

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
the connection details (see below)

`customers` and `invoices` are table directories, each
having a [scan.yml](scan.md) configuration file.

`invoices_*.yml` are user defined [SQL metrics](sql_metrics.md)
that also get executed when a table scan is performed

## warehouse.yml

`warehouse.yml` contains the name of the warehouse and the connection details.

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

The example above shows you how to each environment variables for credentials.
Each environment variable, defined using `env_var(VAR_NAME)`, will automatically be
resolved using the `env_vars.yml` file in your home directory. More on this can be found
in the [cli.yml documentation](cli.md#env-vars.md).

Each warehouse will require different configuration parameters.
See [Warehouse types](warehouse_types.md) to learn how to configure each
type of warehouse.

Soon, Soda project files will also include an optional
link to a Soda cloud account.  A cloud account enables you to push the monitoring
results after each scan and share them with other people in your data organisation.
