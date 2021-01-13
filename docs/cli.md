# Soda's Command Line Interface (CLI)

See [Installation](installation.md) to install the `soda` command

The soda command line is mostly a tool to help you get started with 
your Soda SQL configuration files.
 
To see the list of commands, enter `soda`

```
soda
Usage: soda [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create  Creates a new warehouse directory and prepares credentials in
          your...

  init    Finds tables in the warehouse and based on the contents, creates...
  scan    Computes all measurements and runs all tests on one table.
```
 
| Command | Description |
| ------- | ----------- |
| `soda create ...` | Creates a new warehouse directory and prepares credentials in your ~/.soda/env_vars.yml Nothing will be overwritten or removed, only added if it does not exist yet. |
| `soda init ...` | Finds tables in the warehouse and based on the contents, creates initial scan.yml files |
| `soda scan ...` | Computes all measurements and runs all tests on one table.  Exit code 0 means all tests passed. Non zero exist code means tests have failed or an exception occured. If the project has a Soda cloud account configured, measurements and test results will be uploaded |

To learn about the parameters, use the command line help:
* `soda create --help`
* `soda init --help`
* `soda scan --help`

# Docker 

The Soda SQL CLI interface can also be executed from inside a docker container. This can be done by first building the corresponding docker image: `docker build -t soda/soda-sql .` 
Once this command is finished you can use it the same way as documented in the [CLI section](cli.md#soda39s-command-line-interface-cli)

Examples:
`docker run -it soda/soda-sql --help`
`docker run -it soda/soda-sql create --help`
`docker run -it soda/soda-sql init --help`

# Env vars

To keep your `warehouse.yml` configuration files free of credentials, soda-sql  
supports referencing environment variables like this: `env_var(SOME_ENV_VAR)`

`soda` CLI also includes a convenient mechanism to load your local environment 
variables.  Each `soda` CLI command that reads a warehouse configuration, will 
also read the corresponding environment variables specified in your 
`~/.soda/env_vars.yml`

Example `~/.soda/env_vars.yml`
```yaml
my_project_postgres:
    SNOWFLAKE_USERNAME: someotherexampleusername
    SNOWFLAKE_PASSWORD: someotherexamplepassword

some_other_soda_project:
    POSTGRES_USERNAME: myexampleusername
    POSTGRES_PASSWORD: myexamplepassword
```

The `soda create` command will assist in creating and prepopulating the 
environment variables section in your `~/.soda/env_vars.yml`
