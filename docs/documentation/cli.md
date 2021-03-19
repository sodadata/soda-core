---
layout: default
title: Soda SQL CLI commands
parent: Documentation
nav_order: 2
---

# Soda SQL CLI commands

| Command | Description |
| ------- | ----------- |
| `soda analyze` | Analyzes the contents of your warehouse and automatically prepares a scan YAML file for each table. Soda SQL puts the YAML files in the `/tables` directory inside the warehouse directory. See [Create a scan YAML file]({% link documentation/scan.md %}#create-a-scan-yaml-file) for details.|
| `soda create` | Creates a new `warehouse.yml` file and prepares credentials in your `~/.soda/env_vars.yml`. Soda SQL does not overwrite or remove and existing environment variables, it only adds new. See [Create a warehouse YAML file]({% link documentation/warehouse.md %}#create-a-warehouse-yaml-file) for details. |
| `soda scan` | Uses the configurations in your scan YAML file to prepare, then run SQL queries against the data in your warehouse. See [Run a scan]({% link documentation/scan.md %}#run-a-scan) for details. |

## List of commands

To see a list of Soda SQL command-line interface (CLI) commands, use the `soda` command. 

Command:
```shell
soda
```

Output:
```shell
Usage: soda [OPTIONS] COMMAND [ARGS]...

  Soda CLI version 2.x.xxx

Options:
  --help  Show this message and exit.

Commands:
  analyze  Analyzes tables in the warehouse and creates scan YAML files...
  create   Creates a new warehouse.yml file and prepares credentials in
           your...

  init     Renamed to `soda analyze`
  scan     Computes all measurements and runs all tests on one table.
```

## List of parameters

To see a list of configurable parameters for each command, use the command-line help.
* `soda create --help`
* `soda analyze --help`
* `soda scan --help`


## Go further
* Learn [How Soda SQL works]({% link documentation/concepts.md %}).
* [Install Soda SQL]({% link getting-started/installation.md %}).
* [Set up Soda SQL]({% link getting-started/5_min_tutorial.md %}) and run your first scan.