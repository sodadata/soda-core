<p align="center"><img src="https://github.com/sodadata/docs/blob/main/assets/images/soda-sql-logo.png" alt="Soda logo" width="300" /></p>

<h1 align="center">Soda SQL</h1>
<p align="center"><b>Data testing, monitoring, and profiling for SQL-accessible data.</b></p>

<p align="center">
  <a href="https://github.com/sodadata/soda-sql/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="https://pypi.org/project/soda-sql/"><img alt="Pypi Soda SQL" src="https://img.shields.io/badge/pypi-soda%20sql-green.svg"></a>
  <a href="https://github.com/sodadata/soda-sql/actions/workflows/build.yml"><img alt="Build soda-sql" src="https://github.com/sodadata/soda-sql/actions/workflows/build.yml/badge.svg"></a>
</p>
 <br />
 <br />
Soda SQL is an open-source command-line tool. It utilizes user-defined input to prepare SQL queries that run tests on tables in a data warehouse to find invalid, missing, or unexpected data. When tests fail, they surface "bad" data that you can fix to ensure that downstream analysts are using "good" data to make decisions.
<br />

[Quick start tutorial](https://docs.soda.io/soda-sql/getting-started/5_min_tutorial.html)

## Test your data

If your organization uses data to make decisions, you should always be testing your data. 

- When data comes into a system, you should test it. 
- When data is transformed or otherwise manipulated to fit into an app or other database, you should test it. 
- When data is about to be exported, you should test it. 
- Test to make sure data is unique.
- Test that data is in an expected format, such as date or UUID.
- Test that data doesn’t exceed limits or acceptable parameters. 

## Install Soda SQL

Requirements:
- Python 3.7 or greater
- Pip 21.0 or greater

Install:
```
$ pip install soda-sql-yourdatawarehouse
```

| Data warehouse | Install package |
| -------------- | --------------- |
| Amazon Athena  | soda-sql-athena |
| Amazon Redshift | soda-sql-redshift |
| Apache Hive    | soda-sql-hive  |
| GCP BigQuery   | soda-sql-bigquery |
| MS SQL Server  | soda-sql-sqlserver |
| PostgreSQL     | soda-sql-postgresql |
| Snowflake      | soda-sql-snowflake |

[Full installation instructions](https://docs.soda.io/soda-sql/getting-started/installation.html)

## Use Soda SQL

Install Soda SQL, then complete three basic tasks to start testing your data: 

1. Create and configure a `warehouse.yml` file so that Soda SQL can connect to your data warehouse. 
2. Create and configure a `scan.yml` file to define tests for "bad" data. Choose from a list of predefined metrics to define simple tests – is the table empty? – to more complex tests that borrow from SQL query logic.
3. Run a scan from the command-line to test for "bad" data. Where the tests return “true”, all is well; where a test returns “false”, Soda SQL presents the issues in the command-line output. 

<p align="left"><img src="https://github.com/sodadata/docs/blob/main/assets/images/scan-output-fail.png" alt="scan output" width="700" /></p>

[Full configuration instructions](https://docs.soda.io/soda-sql/getting-started/configure.html)


## Show me the metrics!

**See for yourself!** Follow the [Quick start tutorial](https://docs.soda.io/soda-sql/getting-started/5_min_tutorial.html) to see Soda SQL up and running in five minutes.

This example `scan.yml` file defines **four tests** that Soda SQL runs on data in a table in a data warehouse. 


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
    - distinct
    - unique_count
    - duplicate_count
    - uniqueness
    - maxs
    - mins
    - frequent_values
    - histogram
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
sql_metrics:
    - sql: |
        SELECT sum(volume) as total_volume_us
        FROM CUSTOMER_TRANSACTIONS
        WHERE country = 'US'
      tests:
        - total_volume_us > 5000
```


| Test | Description | Outcome |
| ---- | ----------- | --------------- |
| `tests: duplicate_count == 0` | Tests that there are no duplicate values in the `ID` column of the table. | The test fails if it finds duplicate values.|
| `tests: missing_percentage < 3`| Tests that less than 3% of the values in the `CATEGORY` column match the values set under `missing_values`. | The test fails if more than 3% of the values in the column contain `n/a` or `No category`. |
| `tests: max - min < 20` | Tests that the difference between the highest value and the lowest value in the `SIZE` column is less than 20. | The test fails if the difference exceeds 20. |
| `tests: total_volume_us > 5000` | Tests that the sum total of US transactions in the `CUSTOMER_TRANSACTIONS` column is greater than 5000. | The test fails if the sum total is less than 5000.|

When Soda SQL scans the table, it returns the following scan output in your command-line interface.

```shell
$ soda scan warehouse.yml my_dataset.yml
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

## Go further

- Learn how to automate Soda SQL scans using your [data pipeline orchestration tool](https://docs.soda.io/soda-sql/documentation/orchestrate_scans.html) such as:
   - Apache Airflow
   - AWS Glue
   - Prefect
   - Dagster
   - Fivetran
   - Matillion
   - Luigi
- If you like the goals of this project, we welcome your [contribution](https://docs.soda.io/soda-sql/community.html)! 
- Read more about [How Soda SQL works](https://docs.soda.io/soda-sql/documentation/concepts.html).


