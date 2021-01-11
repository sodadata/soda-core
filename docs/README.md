# soda-sql

SQL-based data testing and data monitoring

It's used for
 * Stopping the pipeline
 * Data monitoring

soda-sql scans make it eas easy to configure declarative data tests in 
yaml configuration files.  All soda-sql configuration files can be 
checked into your version control system as part of your pipeline 
code. On top, you can write your own SQL metrics.

To protect against silent data issues for the consumers of your data,
it's recommended to check your data before and after every data pipeline job.
soda-sql will execute optimized SQL queries to produce measurements.  

While soda-sql is (and will remain) targeted for standalone usage, we'll 
also launch a free cloud account as a companion service to this open 
source project for
 * Storing your metrics over time
 * Enable monitors that track change over time (eg anomaly detection) 
 * Sharing your monitoring results with Analysts and other data roles 

Let's walk you through the main capabilities.

Declare the basic scan metrics and tesst in yaml configuration files 
like this: 
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

Add any SQL query as a metric
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

Run scans as part of your pipeline:
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

Scans produce measurements and test results.  

The goal is to run scans regularly.  Ideally is each time after new data was 
produced in a table.  With the CLI it's possible to schedule scans with cron.  
But ideally you want to trigger a scan at the end of 
the data pipeline job that produced the new data.  That way, problems will be detected 
the soonest.  Soon, we'll add examples how to schedule scans in your favourite
data pipeline orchestration solution like Eg:  

* Airflow
* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you like it so far, encourage us and  
<a class="github-button" href="https://github.com/sodadata/soda-sql" data-icon="octicon-star" data-size="large" aria-label="Star sodadata/soda-sql on GitHub">star soda-sql on GitHub</a> 

Next check out [Getting started](installation.md) for installation and [the tutorial](tutorial.md)
to get your first project going.
