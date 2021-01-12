# Soda SQL

Data testing and monitoring for SQL accessible data.

**What does Soda SQL do?**

 * Stops your pipeline if bad data is detected 
 * Extracts metrics through SQL
 * Data monitoring

**Why Soda SQL?**

To protect against silent data issues for the consumers of your data,
it's recommended to check your data before and after every data pipeline job.
You will know when bad data enters your pipeline.  And you will prevent 
delivery of bad data to downstream consumers. 

**How does Soda SQL work?**

Soda SQL is a Command Line Interface (CLI) and a Python library to measure 
and test your data using SQL.
  
You produce Yaml configuration files that include:
 * SQL connection details
 * What metrics to compute
 * What tests to run on the measurements

Based on those configuration files, Soda SQL will perform scans.  A scan 
performs all measurements and runs all tests associated with one table.  Typically 
a scan is executed after new data has arrived.  All soda-sql configuration files 
can be checked into your version control system as part of your pipeline 
code.

**Show me the money**

Simple metrics and tests can be configured in Yaml configuration files like this: 
`my_warehouse/my_table/scan.yaml` :
```yaml
metrics:
    - row_count
    - missing_count 
    - missing_percentage
    - values_count
    - values_percentage
    - valid_count
    - valid_percentage
    - invalid_count
    - invalid_percentage
    - min
    - max
    - avg
    - sum
    - min_length
    - max_length
    - avg_length
columns:
    ID:
        metrics:
            - distinct
            - duplicate_count
        valid_format: uuid
        tests:
            duplicates: duplicate_count == 0
    CATEGORY:
        missing_values:
            - N/A
            - No category
        tests:
            missing: missing_percentage < 3
    SIZE:
        metrics:
            - 
        tests:
            spread: max - min < 20
```

Any custom SQL metric can be defined as well.  Eg

`my_warehouse/my_table/total_volume_us.yaml` :
```yaml
metrics: 
    - total_volume_us
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

Based on these configuration files, Soda SQL will scan your data 
each time new data arrived like this:

```
$ soda scan ./soda/metrics my_warehouse my_dataset
Soda 1.0 scan for dataset my_dataset on prod my_warehouse
  | SELECT column_name, data_type, is_nullable
  | FROM information_schema.columns
  | WHERE lower(table_name) = 'customers'
  |   AND table_catalog = 'datasource.database'
  |   AND table_schema = 'datasource.schema'
  - 0.256 seconds
Found 4 columns: ID, NAME, CREATE_DATE, COUNTRY
  | SELECT
  |  COUNT(*),
  |  COUNT(CASE WHEN ID IS NULL THEN 1 END),
  |  COUNT(CASE WHEN ID IS NOT NULL AND ID regexp '\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b' THEN 1 END),
  |  MIN(LENGTH(ID)),
  |  AVG(LENGTH(ID)),
  |  MAX(LENGTH(ID)),
  | FROM customers
  - 0.557 seconds
row_count : 23543
missing   : 23
invalid   : 0
min_length: 9
avg_length: 9
max_length: 9

...more queries...

47 measurements computed
23 tests executed
All is good. No tests failed. Scan took 23.307 seconds
```

The next step is to add Soda SQL scans in your favourite
data pipeline orchestration solution like Eg:  

* Airflow
* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you like the goals of this project, encourage us and  
<a class="github-button" href="https://github.com/sodadata/soda-sql" data-icon="octicon-star" data-size="large" aria-label="Star sodadata/soda-sql on GitHub">star soda-sql on GitHub</a> 

Next check out [Getting started](installation.md) for installation and [the tutorial](tutorial.md)
to get your first project going.
