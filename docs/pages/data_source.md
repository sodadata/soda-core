# Data source configuration YAML

This page explains how to configure the connection and other data source details.

### Specifying credentials in data source YAML files

As you can see in the example, credentials can be specified in a data source YAML configuration 
file via environment variables or also via variables that are provided via the CLI or Python API.

In general, the text `${env.SOME_ENV_VAR}` in a data source YAML configuration file will be replaced 
with the value in environment variable `SOME_ENV_VAR`   

Similarly, the text `${var.SOME_ENV_VAR}` in a data source YAML configuration file will be replaced 
with the value provided as variable `SOME_ENV_VAR` in the CLI or Python API

For data sources configured on Soda Cloud, use `${secret.SOME_ENV_VAR}` to refer to secrets in the 
Soda Cloud secret store.

### Postgres

To configure a connection to a postgres database, use this YAML
```yaml
type: postgres
name: postgres_ds
connection:
  host: localhost
  user: ${env.POSTGRES_USERNAME}
  password: ${env.POSTGRES_PASSWORD}
  database: your_postgres_db
```
