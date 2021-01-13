# Soda's Command Line Interface (CLI)

> See [Installation](installation.md) to install the `soda` command.

The soda command line is mostly a tool to help you get started with
your Soda SQL configuration files.

After installing the soda CLI you can enter `soda` in your terminal to see a list of available commands:

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
| `soda create ...` | Creates a new warehouse directory and prepares credentials in your `~/.soda/env_vars.yml` No files will be overwritten or removed, only added when they don't exist yet. |
| `soda init ...` | Finds tables in the configured warehouse and uses its contents to create initial scan.yml files. |
| `soda scan ...` | Computes all measurements and runs all tests on the provided table.  The command will return exit-code `0` when all tests passed. A non-zero exit code means tests have failed or an exception occurred. If the project has a Soda Cloud account configured, measurements and test results will also automatically be uploaded. |

Invoke a command with `--help` to learn more about the command and its available parameters:
* `soda create --help`
* `soda init --help`
* `soda scan --help`

# Env vars

To keep your `warehouse.yml` configuration files free of credentials, soda-sql
supports to reference to environment variables by using the `env_vars(SOME_ENV_VAR)` function.

The `soda` CLI uses a convenient mechanism to load environment variables from your local
user home directory. All environment variables for a warehouse configuration are stored in
the `~/.soda/env_vars.yml` file. Such a file for a user working on two warehouses (`my_project_postgres` and
`some_other_soda_project`) may look like follows:

```yaml
my_project_postgres:
    SNOWFLAKE_USERNAME: someotherexampleusername
    SNOWFLAKE_PASSWORD: someotherexamplepassword

some_other_soda_project:
    POSTGRES_USERNAME: myexampleusername
    POSTGRES_PASSWORD: myexamplepassword
```

Running the `soda create` command will automatically create and pre-populate the
environment variables section in your `~/.soda/env_vars.yml` file.
