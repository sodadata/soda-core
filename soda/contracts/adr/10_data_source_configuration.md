# Data source configuration

In order to verify (Soda) or push (Atlan) a contract, a data source is required.

A contract file will reference a data source by name. Data sources are configured in 
data source YAML files.

Other configurations next to data sources are variables, Soda Cloud connection and Atlan 
connections.

Different environments require different configurations.  The configuration files are 
grouped per environment in a directory.  When performing a contract operation, next to 
the contract files, the environment configuration is passed as a directory.

In an environment configuration directory, the file names are used to determine the file type:

* `data_sources.yml` : Configuration of connections to SQL engines
* `variables.yml` : Configuration of variables for this environment (no credentials)
* `soda.yml` : Configurations for Soda Cloud connectivity
* `atlan.yml` : Configurations for Atlan connectivity
* `vault.yml` : Configurations for loading variables from a secret store.

## Specifying the data source name

A CLI or API operation like Soda's `verify` or Atlan's `push` requires one or more contract files.
Each contract file will need to resolve the data source by name.

A `data_source` is **required** in a contract file and refers to the data source in the data sources 
configuration file.

Eg `./customers.yml`
```yaml
dataset: CUSTOMERS
data_source: snflk_slfc_raw
columns:
    - name: ...
```

### Basic configuration examples

For example: with following file structure...
```
+- .soda
|   +- data_sources.yml
+- contracts
    +- customers.yml
```
And a command line prompt in the root directory, soda can be invoked with 
```
> soda --configuration .soda --contract contracts/customers.yml 
```

In this case the `--configuration .soda` is optional as it will be 1 of 4 default locations 
where the configuration is fetched from:

```
> soda --contract contracts/customers.yml 
```
will by default take the first configuration directory that exists and contains configuration files:
1. ./.soda
2. ./.atlan
3. ${user.home}/.soda
4. ${user.home}/.atlan

*(Note: for the Atlan CLI the precedence of (1) and (2) may be inverted, and the precedence of (3) and (4) may be inverted.)*

### Working with environments

Typically engineers need to work on different environments like (local) development, CI/CD and production.

All configuration files for an environment must be located in a single configuration directory.  That 
environment configuration directory is passed to the CLI or API command.

For example: with following file structure...
```
+- .soda
|   +- dev
|   |   +- data_sources.yml
|   |   +- soda.yml
|   +- cicd
|   |   +- data_sources.yml
|   |   +- soda.yml
|   +- prod
|   |   +- data_sources.yml
|   |   +- soda.yml
+- contracts
|   +- customers.yml
```
Soda can be executing using the cicd environment configurations like this: 
```shell
soda -cfg ./.soda/cicd ./contracts/customers.yml
```

### Referencing data sources in contract files

```yaml
dataset: dim_customers
data_source: snowflake_landing_zone
```

### Data source file format

(1) Example 
```yaml
- type: snowflake
  connection:
      host: 0sd89fs09d8f0s9d8f.snflk.com
      warehouse: XXXL
      username: ${SNOWFLAKE_USERNAME}
      password: ${SNOWFLAKE_PASSWORD}
      role: admin
  data_sources:
      snowflake_landing_zone:
        schema: snowflake_landing_zone
      snowflake_silver:
        schema: SILVER_PRD
      snowflake_gold:
        schema: GOLD_PRODUCTION
```

A data source contains a list of connections.  Each connection has a type.

A connection also has a list of named `data_sources`.  These data source keys must match the names referenced 
in the contract files with `data_source`.

Each data source can specify a specific schema, database or any other structural element of the specific SQL engine.
Data source properties like `database`, `schema` (and in the bigquery case `project` and `datasets`) are dependent 
on the connection type.  For each of these properties we use connection-specific terminology.

And each data source can also overwrite certain connection properties.

### Variables

In all configuration and contract files, variables can be referenced as `${VARIABLE_NAME}`
Variables must be upper case underscore: regex `[A-Z0-9_]+`

(no jinja templating, only variable substitution without spaces)

In the future we may define a `vault.yml` configuration file that specifies how to load environment variables from 
a vault or secret store.

## Identifying datasets

TODO: describe how datasets are identified as a combination 
* configuration folder name
* data source name
* dataset name

Should instead of configuration folder name, a consistent hash of the data source properties be used?
