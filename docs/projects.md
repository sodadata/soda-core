# Projects

A directory with a `soda.yml` configuration file is considered a Soda 
project directory.

## Example

Here's an example Soda project directory structure:

```
+ my_project_snowflake
|   + soda.yml
|   + customers
|   |   + scan.yml        
|   + invoices
|   |   + scan.yml
|   |   + invoices_without_active_country.yml        
|   |   + invoices_with_inactive_products.yml        
```

`soda.yml` contains project level configurations like 
project name and the warehouse connection details.

`customers` and `invoices` are table directories, each 
having a [scan.yml](scan.md) configuration file.

`invoices_*.yml` are [user defined SQL metrics](sql_metrics.md)
that also get executed when a table scan is performed

## soda.yml

Note that we encourage to refer to environment variables for credentials 
in the configurations files as those files are typically checked into a 
version control system. 

For example:
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

Each warehouse will require different configuration parameters.
See [Warehouses](warehouses.md) to learn how to configure each 
type of warehouse. 

The example above shows you can refer to environment variables for 
credentials.  See the next section on [Env vars](env_vars.md) to learn more.

Soon, Soda project files will also include an optional 
link to a Soda cloud account.  A cloud account enable you to push the monitoring 
results after each scan and share them with other people in your data organisation.
