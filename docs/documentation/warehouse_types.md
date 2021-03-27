---
layout: default
title: Set warehouse configurations
parent: Documentation
nav_order: 4
---

# Set warehouse configurations

Soda SQL needs connection details in order to access your [warehouse]({% link documentation/glossary.md %}#warehouse) to scan your data. Each type of warehouse uses different configuration parameters. To set the warehouse configurations in your [warehouse YAML]({% link documentation/warehouse.md %}), use the following example configurations that correspond to each database type that Soda SQL supports.

[Apache Hive](#apache-hive) <br />
[AWS Athena](#aws-athena) <br />
[AWS Redshift](#aws-redshift) <br />
[Google Cloud Platform BigQuery](#gcp-bigquery) <br />
[Microsoft SQL Server](#microsoft-sql-server) <br />
[PostgreSQL](#postgresql) <br />
[Snowflake](#snowflake) <br />


## Apache Hive

```yaml
name: my_hive_project
connection:
    type: hive
    host: localhost
    port: 10000
    username: env_var(HIVE_USERNAME)
    password: env_var(HIVE_PASSWORD)
    database: default
    configuration:
      hive.execution.engine: mr
      mapreduce.job.reduces: 2
...
```

## AWS Athena

```yaml
name: my_athena_project
connection:
    type: athena
    catalog: AwsDataCatalog
    database: sodalite_test
    access_key_id: env_var(AWS_ACCESS_KEY_ID)
    secret_access_key: env_var(AWS_SECRET_ACCESS_KEY)
    role_arn: an optional IAM role arn to be assumed
    region: eu-west-1
    staging_dir: <YOUR STAGING PATH IN AWS S3>
...
```

| Property | Description | Required |
| -------- | ----------- | -------- |
| type | `snowflake` | Required |
| catalog | | Optional (default is `AwsDataCatalog`) |
| database | | Required |
| staging_dir | | Required |
| access_key_id | | Optional |
| secret_access_key | | Optional |
| role_arn | | Optional |
| region | | Optional |


## AWS Redshift

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


## GCP BigQuery

```yaml
name: my_bigquery_project
connection:
    type: bigquery
    # YOUR BIGQUERY SERVICE ACCOUNT INFO JSON FILE
    account_info_json: >
      {
        "type": "service_account",
        "project_id": "...",
        "private_key_id": "...",
        "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
        "client_email": "user@project.iam.gserviceaccount.com",
        "client_id": "...",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/..."
      }
    dataset: sodasql
...
```

## Microsoft SQL Server

Note: This connection is experimental.

```yaml
name: my_sqlserver_project
connection:
  type: sqlserver
  host: <YOUR SQLServer HOSTNAME>
  username: env_var(SQL_SERVER_USERNAME)
  password: env_var(SQL_SERVER_PASSWORD)
  database: master
  schema: dbo
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

## Snowflake

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