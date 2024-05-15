
Make `data_source` name reference required in the contract YAML.

customers.yml
```yaml
dataset: DIM_CUSTOMERS
data_source: snowflake_raw
```

Require a data_sources.yml (list of connections, containing a list of named data sources)


${user.home}/.soda/{projectname}/dev

.soda
 + data_sources.yml 
 + variables.yml
 + atlan.yml
 + soda.yml
 + prod
    + data_sources.yml 
    + variables.yml
    + atlan.yml
    + soda.yml
 + cicd
data_contracts
 + .dc.yml

Each tool invocation needs a config folder.  The folder is scanned for known file names:
    + data_sources.yml 
    + variables.yml
    + atlan.yml
    + soda.yml
The file name determines the file type

By default these folders is used as the config folder
./.soda
./.atlan
${user.home}/.soda
${user.home}/.atlan

```shell
tool -cd contract-files
```


```shell
tool -cfg --configuration .soda/prod -cd contract-files
```

```yaml
- type: snowflake
  properties:
      host: 0sd89fs09d8f0s9d8f.snflk.com
      warehouse: ${SNOWFLAKE_WAREHOUSE}
      username: ${SNOWFLAKE_USERNAME}
      password: ${SNOWFLAKE_USERNAME}
      role: admin
  data_sources:
      snowflake_landing_zone_default:
      snowflake_landing_zone_prod:
        schema: RAW
      snowflake_landing_zone_prod_big:
        schema: RAW
        warehouse: XXXL
      snowflake_landing_zone_cicd:
        schema: CICD
- type: snowflake
  properties:
      host: 0sd89fs09d8f0s9d8f.snflk.com
      warehouse: ${SNOWFLAKE_WAREHOUSE}
      username: ${SNOWFLAKE_USERNAME}
      password: ${SNOWFLAKE_USERNAME}
      role: admin
  data_sources:
      snowflake_landing_zone_prod:
        schema: RAW
      snowflake_landing_zone_prod_big:
        schema: RAW
        warehouse: XXXL
      snowflake_landing_zone_cicd:
        schema: CICD
- type: postgres
  properties:
      host: localhost
      username: ${POSTGRES_USERNAME}
      password: ${POSTGRES_USERNAME}
  data_sources:
      snowflake_landing_zone_prod:
        database: DB
        schema: RAW
      snowflake_landing_zone_cicd:
        schema: CICD
```

Commands have as input:
* List of data source files -> uniquely named data sources
* List of contract files

Default command on a contract file
```shell
tool -contract ./customers.yml
```

```shell
tool --data-source-mapping snowflake_raw:snowflake_landing_zone_prod_big \
     --data-source-mapping snowflake_landing_zone_cicd:snowflake_landing_zone_prod_big \
     --contract ./customers.yml
```


environments.yml (optional, recommended for cicd)
```yaml
prod:
  data_source_mappings:
    snowflake_raw: snowflake_raw_local_dev
    postgres_refined: postgres_refined_dev
  variables:
    to: do
cicd:
  data_source_mappings:
    snowflake_raw: snowflake_raw_local_dev
    postgres_refined: postgres_refined_dev
  variables:
    to: do
```

```shell
tool --environment prod --contract ./customers.yml
```
