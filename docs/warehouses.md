# Warehouses

Warehouse configuration information are stored in the `~/.soda/profiles.yml` file.

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