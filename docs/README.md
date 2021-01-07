# soda-sql

SQL-based data quality testing for Data Engineers

It's used for
 * Data testing
 * Stopping the pipeline
 * Data monitoring
 * Data validation

soda-sql scans make it eas easy to configure declarative data tests in yaml files.
All soda-sql configuration files can be checked into your version control 
system as part of your pipeline code. On top, you can write your own 
SQL metrics.

To protect against silent data issues for the consumers of your data,
it's recommended to check your data before and after every data pipeline job.
soda-sql will execute optimized SQL queries to produce measurements.  

While soda-sql is (and will remain) targetted for standalone usage, we'll 
also launch a free cloud account as a companion service to this open 
source project for
 * Storing your metrics over time
 * Enable monitors that track change over time (eg anomaly detection) 
 * Sharing your monitoring results with Analysts and other data roles 

Let's walk you through the main capabilities:

`my_warehouse/my_dataset/scan.yaml` :
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
...
```

Customize metrics for each column individually:
```yaml
...
columns:
    ID:
        metrics:
            - distinct
            - non_unique_count
        valid_format: uuid
        tests:
            - non_unique_count == 0
    CATEGORY:
        missing_values:
            - N/A
            - No value
        valid_values:
            - High
            - Medium
            - Low
        tests:
            missing_percentage_2 : missing_percentage <  3.0
            invalid_count : invalid_count == 0
```

Add custom SQL query metrics

`my_warehouse/my_dataset/customers_with_expired_zip_code.yaml` :
```yaml
id: customers_with_expired_zip_code
name: Customers with expired zip code
type: failed_rows
sql: |

SELECT
  MD.id,
  MD.name,
  MD.zipcode,
FROM my_dataset as MD
  JOIN zip_codes as ZC on MD.zipcode = ZC.code
WHERE ZC.date_expired IS NOT NULL
```

Add scans to your data pipeline with the command line interface:
```
$ soda scan -env prod ./soda/metrics my_warehouse my_dataset
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

Sending measurements to cloud.soda.io...
Done - Took 23.307 seconds
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

Next check out [Getting started](getting_started.md) for installation and [the tutorial](tutorial.md)
to get your first project going.

