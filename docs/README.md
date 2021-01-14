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

As input, Soda SQL uses Yaml configuration files that include:
 * SQL connection details
 * What metrics to compute
 * What tests to run on the measurements

Based on those configuration files, Soda SQL will perform scans.  A scan
performs all measurements and runs all tests associated with one table.  Typically
a scan is executed after new data has arrived.  All soda-sql configuration files
can be checked into your version control system as part of your pipeline
code.

> Want to try Soda SQL? Head over to our ['5 minute tutorial'](https://docs.soda.io/soda-sql/#/5_min_tutorial) and get started straight away!

**Show me the money**

Simple metrics and tests can be configured in Yaml configuration files called `scan.yml`. An example
of the contents of such a file:

```yaml
# ./my_warehouse/my_table/scan.yml
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
      - distinct
    tests:
      spread: max - min < 20
```

Metrics aren't limited to the ones defined by Soda SQL. You can create your own custom SQL metric definitions by
creating yml files.

```yaml
# ./my_warehouse/my_table/total_volume_us.yml
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

```shell
$ soda scan ./my_warehouse my_table
  | Soda CLI version 2.0.0 beta
  | Scanning my_table in ./my_warehouse ...
  | Environment variable POSTGRES_PASSWORD is not set
  | Executing SQL query:
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE lower(table_name) = 'my_table'
  AND table_catalog = 'sodasql'
  AND table_schema = 'public'
  | SQL took 0:00:00.029199
  | 6 columns:
  |   id character varying
  |   name character varying
  |   size integer
  |   date date
  |   feepct character varying
  |   country character varying
  | Query measurement: schema = id character varying, name character varying, size integer, date date, feepct character varying, country character varying
  | Executing SQL query:
SELECT
  COUNT(*),
  COUNT(id),
  MIN(LENGTH(id)),
  MAX(LENGTH(id)),
  COUNT(name),
  MIN(LENGTH(name)),
  MAX(LENGTH(name)),
  COUNT(size),
...
  | missing_count(country) = 0
  | values_percentage(country) = 100.0
  | All good. 38 measurements computed. No tests failed.
```

The next step is to add Soda SQL scans in your favorite
data pipeline orchestration solution like:

* Airflow
* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi
* ...

If you like this project, encourage us and star and follow
<a class="github-button" href="https://github.com/sodadata/soda-sql" data-icon="octicon-star" data-size="large" aria-label="Star sodadata/soda-sql on GitHub">soda-sql on GitHub</a>

> Next, head over to our ['5 minute tutorial'](https://docs.soda.io/soda-sql/#/5_min_tutorial) and get your first project going!
