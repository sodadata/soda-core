# Warehouses

## Profiles

The file `~/.soda/profiles.yml` is used by the `soda` CLI to obtain connection details to  warehouses (databases).
This prevents that any credentials are checked into version control as part of the scan and custom sql configuration files.

Example:
```
customer:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      username: xxx
      password: xxx
      database: your_dev_db
    prod:
      type: snowflake
      username: xxx
      password: xxx
      account: YOURACCOUNTNAME.eu-central-1
      warehouse: DEMO_WH
      database: YOURDATABASE
      schema: PUBLIC
    prod:
      type: athena
      database: '***'
      access_key_id: '***'
      secret_access_key: '***'
      # role_arn: ***
      # region: eu-west-1
      workDir: '***'
```

The top level keys define different profiles, in the following example, "default", "test", and "prod":

```yaml
default:
...
test:
...
prod:
...
```

You should have at least a "default" profile key.

Configurations for individual warehouses are stored under the key "outputs". Each "output" represents a different connection to a data store.

The "target" key selects the active warehouse configuration (the one that will be used in the scan).

```yaml
default:
  target: athena-dev
  outputs:
    athena-dev:
    ...
    postgres-dev:
    ...
    snowflake-dev:
    ...
...
```

In the example above, the active configuration is "athena-dev".

## Snowflake

```yaml
default:
  target: snowflake-dev
  outputs:
    snowflake-dev:
      type: snowflake
      username: <YOUR SNOWFLAKE USERNAME>
      password: <YOUR SNOWFLAKE PASSWORD>
      account: <YOUR SNOWFLAKE ACCOUNT NAME>
      warehouse: DEMO_WH
      database: FUNDS
      schema': PUBLIC
...
```

## AWS Athena

```yaml
default:
  target: athena-dev
  outputs:
    athena-dev:
      type: athena
      database: sodalite_test
      access_key_id: <YOUR AWS ACCESS KEY>
      secret_access_key: <YOUR AWS SECRET ACCESS KEY>
      region_name: eu-west-1
      staging_dir: <YOUR STAGING PATH IN AWS S3>
...
```

## GCP BigQuery

```yaml
default:
  target: bigquery-dev
  outputs:
    bigquery-dev:
      type: bigquery
      account_info: <PATH TO YOUR BIGQUERY ACCOUNT INFO JSON FILE>
      dataset: sodalite
...
```

## PostgreSQL

```yaml
default:
  target: postgres-dev
  outputs:
    postgres-dev:
      type: postgres
      host: localhost
      username: sodasql
      password: sodasql
      database: sodasql
      schema: public
...
```

## Redshift

```yaml
default:
  target: redshift-dev
  outputs:
    redshift-dev:
      type: redshift
      host: <YOUR AWS REDSHIFT HOSTNAME>
      username: soda
      password: <YOUR AWS REDSHIFT PASSWORD>
      database: soda_agent_test
      schema: public
...
```

It's also possible to connect using AWS CREDENTIALS instead of a user name and password:

```yaml
default:
  target: redshift-dev
  outputs:
    redshift-dev:
      type: redshift
      host: <YOUR AWS REDSHIFT HOSTNAME>
      database: soda_agent_test
      schema: public
      access_key_id: <YOUR AWS ACCESS KEY>
      secret_access_key: <YOUR AWS SECRET ACCESS KEY>
      region_name: eu-west-1
...
```
```