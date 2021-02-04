# Soda SQL

Data testing, monitoring and profiling for SQL accessible data.
 
**What does Soda SQL do?**

Soda SQL allows you to

 * Stop your pipeline when bad data is detected
 * Extract metrics and column profiles through super efficient SQL
 * Full control over metrics and queries through declarative config files

**Why Soda SQL?**

To protect against silent data issues for the consumers of your data,
it's best-practice to profile and test your data:

 * as it lands in your warehouse,
 * after every important data processing step
 * right before consumption.

This way you will prevent delivery of bad data to downstream consumers.
You will spend less time firefighting and gain a better reputation.

**How does Soda SQL work?**

Soda SQL is a Command Line Interface (CLI) and a Python library to measure
and test your data using SQL.

As input, Soda SQL uses YAML configuration files that include:
 * SQL connection details
 * What metrics to compute
 * What tests to run on the measurements

Based on those configuration files, Soda SQL will perform scans.  A scan
performs all measurements and runs all tests associated with one table.  Typically
a scan is executed after new data has arrived.  All soda-sql configuration files
can be checked into your version control system as part of your pipeline
code.

> Want to try Soda SQL? Head over to our ['5 minute tutorial'](https://docs.soda.io/soda-sql/getting-started/5_min_tutorial.html) and get started straight away!

**"[Show me the money](https://www.youtube.com/watch?v=1-mOKMq19zU)"**

Simple metrics and tests can be configured in YAML configuration files called `scan.yml`. An example
of the contents of such a file:

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
            duplicate_count == 0
    CATEGORY:
        missing_values:
            - N/A
            - No category
        tests:
            missing_percentage < 3
    SIZE:
        tests:
            max - min < 20
```

Metrics aren't limited to the ones defined by Soda SQL. You can create your own custom SQL metric definitions
with a simple yml file.

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

```bash
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

The next step is to add Soda SQL scans in your favorite
data pipeline orchestration solution like:

* Airflow
* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you like the goals of this project, encourage us! Star [sodadata/soda-sql on Github](https://github.com/sodadata/soda-sql).

> Next, head over to our ['5 minute tutorial'](https://docs.soda.io/soda-sql/getting-started/5_min_tutorial.html) and get your first project going!
