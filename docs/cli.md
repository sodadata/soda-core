# Soda's Command Line Interface (CLI)

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

## Configuration directory layout



## Execute a scan 

TODO doc commands of the command line

`soda scan ...`
