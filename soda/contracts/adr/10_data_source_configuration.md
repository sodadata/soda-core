# Data source configuration

A list of data sources are declared in data source YAML files.

In order to verify or push a contract, a data source is required.

Contracts may be used in different environments. In order to use a contract in different
environments, the data source must be made configurable.  A contract may also have a default data source.

In order to overwrite or specify the data source, 2 options are explained below:
* Specify the environment as a parameter
* Use a variable to specify a different data source

## Data source file format

(1) Example
```yaml
type: snowflake

connection:
  # Connection properties
  host: ... (prod host)
  port: 443
  connection_uri: jdbc://username@password:...:443
  username: ${SNOWFLAKE_USERNAME}
  password: ${SNOWFLAKE_PASSWORD}

# declaring a set of named data sources
data_sources:

  # Defines data source snowflake_slfc_raw
  - name: snflk_slfc_raw
    database: db_name1
    schema: SLFC_RAW
    warehouse: WH
    role: ...

  # Defines data source segment_events
  - name: segment_events
    database: SEG
    schema: SGM
    warehouse: WH
    role: ...
```

(1.1) Terminology: We refer to this type of file as a **data source** file or **data source configuration** file.
And we will **not** refer to these as database, warehouse or connection files.

(2) Properties like `database`, `schema` and in the bigquery case `project` and `datasets` are
dependent on the connection type.  For each of these properties we use connection-specific
terminology.

## Default data source resolving

(3) A CLI or API operation like Soda's `verify` or Atlan's `push` requires one or more contract files.
Each contract file has a default way to resolve the data source based on relative file location.

(4) These source files will be found automatically, and used in this order of precedence (top-most is highest precedence):
* {contract_file_dir}/data_sources.yml
* {contract_file_dir}/../data_sources.yml
* {contract_file_dir}/../../data_sources.yml
* {user.home}/.soda/data_sources.yml
* {user.home}/.atlan/data_sources.yml

(5) Apart from the default data source file paths, users can specify other data source file paths or
directories as CLI or API parameters:
* CLI parameter `--data-source` or `-d` for a single data source file path
* API method `.data_source(file_path: str)` for a single data source file path
* CLI parameter `--data-sources-dir` or `-dd` for a directory containing data source files
* API method `.data_sources_dir(dir_path: str)` for a directory containing data source files

(6) By default, if there is only one data source specified in the configuration files, that data
source is used without the need for specifying the `data_source` reference property in the
contract file.

The motivation for this is to keep the cognitive load to a minimal for initial use cases where people are learning.

## Specifying environments

(7) In case users want to work with different environments, we allow for the `environment` parameter
to be specified in the CLI and API.

Typical environments are: `dev`, `prod`, `cicd`.  Environment names must be matching regex `[a-z_]+`.  The motivation
to only allow for lower case is to prevent upper-lower-case errors.

* CLI parameter `--environment` or `-e` to specify the environment name
* API method `.environment(environment: str)` for the environment name

(8) The data sources can have a property `environment` that has to match with the specified environment.  For example:

```yaml
type: snowflake

connection:
  # Connection properties
  host: ... (prod host)
  port: 443
  connection_uri: jdbc://username@password:...:443
  username: ${SNOWFLAKE_USERNAME}
  password: ${SNOWFLAKE_PASSWORD}

# declaring a set of named data sources
data_source:

  # Defines data source snowflake_slfc_raw in the default environment
  - name: snflk_slfc_raw
    database: db_name1
    schema: SLFC_RAW
    warehouse: WH
    role: ...
    use_quotes: true
    variables:
      TABLE_PREFIX: SR_

  # Defines data source snflk_slfc_raw in the default environment
  - name: snflk_slfc_raw
    environment: cicd
    database: db_name2
    schema: cicd_schem
    warehouse: ...
    role: ...
    variables:
      TABLE_PREFIX: SEGM_

variables:
  DB_PREFIX: SWFLK_
```

## Identifying datasets

TODO: describe how we match datasets.

TODO: This should be based on a consistent hash of the data source properties.

### Configuring variables

TODO: Variables should used to specify credentials...

TODO: Variables can used in the contract file to specify a different data source name...

## Quoting

TODO

## Example: simplest configuration using defaults

Contract file `./customers.yml`
```yaml
dataset: CUSTOMERS
columns:
    - name: ...
```

With CLI command:
```shell
command -c ./customers.yml
```

will match the single data source in

Data source file `./data_sources.yml`
```yaml
type: ...
data_sources:
  - name: the_single_ds
    database: ...
    username: ***
    password: ***
    other: datasource properties
```

## Example: Specifying a data source name

Contract file `./customers.yml`
```yaml
dataset: CUSTOMERS
data_source: postgres_segment_ds
columns:
    - name: ...
```

With CLI command:
```shell
command -c ./customers.yml
```

will match the `postgres_segment_ds` data source in

Data source file `../data_sources.yml`
```yaml
type: postgres
data_sources:
  - name: postgres_segment_ds
    database: ...
    username: ***
    password: ***
    other: datasource properties
  - name: postgres_snowflake_ds
    database: ...
    username: ***
    password: ***
    other: datasource properties
```

## Example: Specifying a data source & environment

Contract file `./customers.yml`
```yaml
dataset: CUSTOMERS
data_source: postgres_segment_ds
columns:
    - name: ...
```

With CLI command:
```shell
command -c ./customers.yml -e cicd
```

will match the second `postgres_segment_ds` data source in

Data source file `../data_sources.yml`
```yaml
type: postgres
data_sources:
  - name: postgres_segment_ds
    database: ...
    username: ***
    password: ***
    other: datasource properties
  - name: postgres_segment_ds
    environment: cicd
    database: ...
    username: ***
    password: ***
    other: datasource properties
```

## Example: Specifying a data source file

Contract file `./customers.yml`
```yaml
dataset: CUSTOMERS
data_source: postgres_segment_ds
columns:
    - name: ...
```

With CLI command:
```shell
command -c ./customers.yml -d ../cfg/postgres_data_sources.yml
```

will match the `postgres_segment_ds` data source in

Data source file `../cfg/postgres_data_sources.yml` (non-default name & directory!)
```yaml
type: postgres
data_sources:
  - name: postgres_segment_ds
    database: ...
    username: ***
    password: ***
    other: datasource properties
```
