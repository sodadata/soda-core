---
layout: default
title: Warehouse types
parent: Documentation
nav_order: 4
---

# Warehouse types

Warehouses are configured as part of a [Soda warehouse configuration file]({% link documentation/warehouse.md %})
This section explains the concrete connection properties for each warehouse type.

## Snowflake

Example configuration

```yaml
name: my_snowflake_project
connection:
    type: snowflake
    username: env_var(SNOWFLAKE_USERNAME)
    password: env_var(SNOWFLAKE_PASSWORD)
    account: YOUR_SNOWFLAKE_ACCOUNT.eu-west-1
    warehouse: YOUR_WAREHOUSE
    database: YOUR_DATABASE
    schema: PUBLIC
...
```

| Property | Description | Required |
| -------- | ----------- | -------- |
| type | `snowflake` | Required |
| username |  | Required |
| password |  | Required |
| account | Eg YOUR_SNOWFLAKE_ACCOUNT.eu-west-1 | Required |
| warehouse |  | Required |
| database |  | Required |
| schema |  | Required |

## AWS Athena

Example

```yaml
name: my_athena_project
connection:
    type: athena
    database: sodalite_test
    access_key_id: env_var(AWS_ACCESS_KEY_ID)
    secret_access_key: env_var(AWS_SECRET_ACCESS_KEY)
    role_arn: an optional IAM role arn to be assumed
    region: eu-west-1
    staging_dir: <YOUR STAGING PATH IN AWS S3>
...
```

## GCP BigQuery

```yaml
name: my_bigquery_project
connection:
    type: bigquery
    account_info: <PATH TO YOUR BIGQUERY ACCOUNT INFO JSON FILE>
    dataset: sodasql
...
```

## PostgreSQL

```yaml
name: my_postgres_project
connection:
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
name: my_redshift_project
connection:
    type: redshift
    host: <YOUR AWS REDSHIFT HOSTNAME>
    username: soda
    password: <YOUR AWS REDSHIFT PASSWORD>
    database: soda_agent_test
    schema: public
    access_key_id: env_var(AWS_ACCESS_KEY_ID)
    secret_access_key: env_var(AWS_SECRET_ACCESS_KEY)
    role_arn: an optional IAM role arn to be assumed
    region: eu-west-1
...
```

## Spark SQL

Coming soon
