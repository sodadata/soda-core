# Data source configuration

In order to verify or push a contract, a data source is required.

Data sources are configured in data source YAML files.

Contracts may be used in different environments. In order to use a contract in different
environments, the data source must be made configurable.  A contract may also have a 
default data source.

To specify the data source for a contract, these are the ways to specify the data source 
for a contract:
1. Specify the data source name in the contract
2. Use a variable to specify a the data source 
3. Specify or overwrite the data source name as a parameter (in CLI or API)

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
      environment: prod
      database: db_name1
      schema: SLFC_RAW
      warehouse: WH
      role: ...
    # Defines data source segment_events
    - name: snflk_slfc_raw
      database: SEG
      schema: SGM
      warehouse: WH
      role: ...
```





./environments.yml
```yaml
local:
  connection:  
  datasets:
  schema: 
      
  data_source_file: ${USER_HOME}/.ds/snowflake.ds.yml
  data_source_name: snflk_slfc_raw
cicd:
    
prod:
```

```shell
{TOOLNAME} -c ./customers.dc.yml -environment prod
```

```shell
soda -cfg ./config.yml -contract ./contracts/customers.yml

soda -cfg ./other.config.yml -contract ./contracts/customers.yml
```

Terminology: We refer to this type of file as a **data source** file or **data source configuration** file.
And we will **not** refer to these as database, warehouse or connection files.

Properties like `database`, `schema` and in the bigquery case `project` and `datasets` are
dependent on the connection type.  For each of these properties we use connection-specific
terminology.

## Specifying the data source name

A CLI or API operation like Soda's `verify` or Atlan's `push` requires one or more contract files.
Each contract file will need to resolve the data source by name.

The data source name can be specified in a contract file `./customers.yml`
```yaml
dataset: CUSTOMERS
data_source: snflk_slfc_raw
columns:
    - name: ...
```

Or the data source name can be specified as a command parameter:
```yaml
dataset: CUSTOMERS
columns:
    - name: ...
```
```shell
toolname -ds snflk_slfc_raw -c ./customers.yml
```

The data source name can also be overwritten as a command parameter:
```yaml
dataset: CUSTOMERS
data_source: snflk_slfc_raw
columns:
    - name: ...
```
```shell
toolname -ds cicd_snflk_slfc_raw -c ./customers.yml
```
In this case the data source `cicd_snflk_slfc_raw` will be used.

## Resolving the data source name

Any operation can be provided with a collection of data source configuration files.  
All data source names provided must be unique and if not, an error should be produced.

These source files will be found automatically:
* {contract_file_dir}/data_sources.yml
* {contract_file_dir}/../data_sources.yml
* {contract_file_dir}/../../data_sources.yml
* {user.home}/.data_sources/*.yml
* data_sources/*.yml

Apart from the default data source file paths, users can specify other data source file paths or
directories as CLI or API parameters:
* CLI parameter `--data-source` or `-d` for a single data source file path
* API method `.data_source(file_path: str)` for a single data source file path
* CLI parameter `--data-sources-dir` or `-dd` for a directory containing data source files
* API method `.data_sources_dir(dir_path: str)` for a directory containing data source files

## Recommended contract file structure



```
+ README.md (generic file indicating this is the root of a repo)
+ data.environments.yml
      
+ data_sources
|  + snowflake_prod.ds.yml         # data source file
|  + snowflake_cicd.ds.yml         # data source file
|  + postgres_local.ds.yml         # data source file
+ contracts                        # tools may recognize this folder name in a project 
|  + user_defined_project_name
|  |  + snowflake_cicd.ds.yml         # data source file
|  |  + postgres_local.ds.yml         # data source file
project_name            # user defined project name
|  + snowflakecicd              # directory grouping all contracts in cicd data source
|  |  + customers.yml  # contract in cicd data source 
```

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
