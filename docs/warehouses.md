# Warehouses

## Profiles

File `~/.soda/profiles.yml` is used by the `soda` CLI to obtain connection details to 
warehouses (databases).  This prevents that any credentials are checked into version control 
as part of the scan and custom sql configuration files.

Eg:

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

## Snowflake

Snowflake profile configuration properties

| Property | Description | Required | Example |
| --- | --- | --- | --- |
| username | The username | Required | johndoe |
| password | The password | Required |         |
| account | The account host | Required | YOURACCOUNTNAME.eu-central-1 |
| warehouse | | | DEMO_WH |
| database | | | YOURDATABASE |
| schema | | default: PUBLIC | PUBLIC |

## AWS Redshift

TODO

### username / password authentication

`username`
`password`

### aws credentials authentication

`access_key_id`
`secret_access_key`
`session_token`
`region` (default `eu-west-1`)
`role_arn`

### Obtaining cluster credentials

This authentication mechanism is used if `username` is specified and `password` is not

optionally aws credentials as above

temp password is obtained with get_cluster_credentials: 
https://docs.aws.amazon.com/redshift/latest/APIReference/API_GetClusterCredentials.html

## AWS Athena

TODO

## GCP BigQuery

TODO

## PostgreSQL

TODO