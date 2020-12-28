# Soda's Command Line Interface (CLI)

## Installation

### From PyPI

```
$ pip install soda-sql
```

### Manually

- Clone the repository from Github.

```
$ git clone https://github.com/sodadata/soda-sql.git
```

- Change to the `soda-sql` directory:

```
$ cd soda-sql
```

- Create a new virtual environment for `soda-sql`, .e.g, "soda_env":

```
$ python -m venv soda_env
```

- Activate the environment:
```
$ source soda_env/bin/activate
```

- Install the package:
```
$ pip install .
```

- Run the CLI application:

```
$ soda-sql
Usage: soda-sql [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  check
  initialize
  scan
```

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
