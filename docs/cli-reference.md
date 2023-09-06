# Soda Core CLI commands 


| Command               | Description |
| --------------------- | ----------- |
| `soda ingest` | Ingests dbt test results to display in Soda Cloud.  |
| `soda scan` | Uses the configurations in your checks YAML file to prepare, then run SQL queries against the data in your data source. See [Run a Soda Core scan](/docs/scan-core.md) for details. |
| `soda test-connection` | Tests the connection to the data sources for which you configured a connection in your configuration YAML file. |
| `soda update-dro` | Updates a distribution reference file that a [distribution check](https://docs.soda.io/soda-cl/distribution.html) uses. |

## List of commands

To see a list of Soda Core command-line interface (CLI) commands, use the `soda` command.

Command:
```shell
soda
```

Output:
```shell
Usage: soda [OPTIONS] COMMAND [ARGS]...

  Soda Core CLI version 3.0.0xx

Options:
  --help  Show this message and exit.

Commands:
  scan    runs a scan
  update-dro  updates a distribution reference file
```

## List of command options

To see a list of configurable options for each command, use the command-line help.
```shell
soda ingest --help
soda scan --help
soda test-connection --help
soda update-dro --help
```

Refer to [Add scan options](/docs/scan-core.md#add-scan-options) for more information.
