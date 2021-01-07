# 5 min tutorial

**Prerequisites**
 * Python 3.7+
 * `soda` CLI.  See [Getting started](getting_started.md) to get the `soda` CLI installed.
 * [Docker](https://docs.docker.com/get-docker/) which will be used in this tutorial to 
   launch an example postgres database for soda-sql to test. If you already have a postgres 
   running on your machine, feel free to use that one.

If at any time during this tutorial you get stuck, speak up 
in the [getting-started Slack channel](slack://channel?id=C01HYL8V64C&team=T01HBMYM59V) or 
[post an issue on GitHub](https://github.com/sodadata/soda-sql/issues/new)

TODO soon, we'll add instructions to launch a vanilla postgres database 
and load demo data to scan. 

#### 1\) Check your `soda` installation 

Open a command line and enter `soda` to check your soda command line tool.

```
$ soda
Usage: soda [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  create  Creates a new project directory and prepares credentials in your...
  init    Initializes scan.yml files based on profiling tables found in the...
  scan    Computes all measurements and runs all tests on one table.
  verify  Dry run to verify if the configuration is ok.
```

If you don't get this output, check out [getting started](getting_started.md) 
for installation instructions or [reach out to us](community.md)

#### 2) Start the postgres docker container

This postgres will act as your data warehouse.
This script will place the postgres files in your `~/soda_sql_tutorial_postgres/`
Feel free to use a different location   

```shell script
docker run \
    -p 5432:5432 \
    -v ~/soda_sql_tutorial_postgres/:/var/lib/postgresql/data \
    -e POSTGRES_USER=sodasql \
    -e POSTGRES_DB=sodasql \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    postgres:9.6.17-alpine
```

When you see output like this, it means your postgres database is ready to go:
```
...
LOG:  database system is ready to accept connections
LOG:  autovacuum launcher started
```

#### 3\) Load example data 



#### 2\) Create a warehouse 

Run `soda create` CLI command to create a warehouse configuration file

The `soda create` will only create and append config files.  It will 
never overwrite or delete things in existing files. So you should not be scared 
it will delete things.  The command reports what it finds and does.

Imagine that you're working on a customers postgres database. 

```
$ soda create ~/terpentine postgres
  | Soda CLI version 2.0.0 beta
  | Creating project dir /Users/tom/my_project ...
  | Creating project file /Users/tom/my_project/soda.yml ...
  | Creating /Users/tom/.soda/env_vars.yml with example env vars
  | Please review and update the 'my_project_postgres' environment variables in ~/.soda/env_vars.yml
  | Then run 'soda init /Users/tom/my_project'
```

To learn more on this command, see [soda create](cli.md#create) 

Next, review and update the 2 files that have been created:
 * `~/.soda/profiles.yml`
 * `~/.soda/profiles.yml`

See
 * [soda init command](cli.md#create) to learn more about `soda create`
 * [Warehouses](warehouse_types.md) to learn more about ~/.soda/profiles.yml
 * [Project](warehouse.md) to learn more about soda_project.yml

#### 3\) Initialize the Soda project with scan.yml files 

Use the `init` helps to create a `scan.yml` for each table in your warehouse
with good defaults that you can customize.

```
$ soda init ~/tmp/my_project 
  | Soda CLI version 2.0.0 beta
  | Initializing /Users/tom/tmp/my_project ...
  | Environment variable POSTGRES_PASSWORD is not set
  | Querying warehouse for tables
  | Executing SQL query: 
SELECT table_name 
FROM information_schema.tables 
WHERE lower(table_schema)='public'
  | SQL took 0:00:00.020269
  | Creating table directory /Users/tom/tmp/my_project/demodata
  | Creating /Users/tom/tmp/my_project/demodata/scan.yml ...
  | Next, run 'soda scan /Users/tom/tmp/my_project demodata' to calculate measurements and run tests
```

#### 4\) Review and update the generated scan.yaml files

Customize the generated scan.yml files to your needs.  See [scan.yaml](scan.md) 
for the details.

`~/tmp/my_project/demodata/scan.yml`
```
table_name: demodata
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
```

#### 5\) Run a scan 

Now you can run a scan on any of the tables created like this:

```
$ soda create ~/tmp/my_project postgres
  | Soda CLI version 2.0.0 beta
  | Creating project dir /Users/tom/tmp/my_project ...
  | Creating project file /Users/tom/tmp/my_project/soda.yml ...
  | Creating /Users/tom/.soda/env_vars.yml with example env vars
  | Please review and update the 'my_project_postgres' environment variables in ~/.soda/env_vars.yml
  | Then run 'soda init /Users/tom/tmp/my_project'
(.venv) [~/Code/soda/sodasql] soda init ~/tmp/my_project 
  | Soda CLI version 2.0.0 beta
  | Initializing /Users/tom/tmp/my_project ...
  | Environment variable POSTGRES_PASSWORD is not set
  | Querying warehouse for tables
  | Executing SQL query: 
SELECT table_name 
FROM information_schema.tables 
WHERE lower(table_schema)='public'
  | SQL took 0:00:00.020269
  | Creating table directory /Users/tom/tmp/my_project/demodata
  | Creating /Users/tom/tmp/my_project/demodata/scan.yml ...
  | Next run 'soda scan /Users/tom/tmp/my_project demodata' to calculate measurements and run tests
(.venv) [~/Code/soda/sodasql] soda scan ~/tmp/my_project demodata
  | Soda CLI version 2.0.0 beta
  | Scanning demodata in /Users/tom/tmp/my_project ...
  | Environment variable POSTGRES_PASSWORD is not set
  | Executing SQL query: 
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE lower(table_name) = 'demodata' 
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
  MIN(size),
  MAX(size),
  AVG(size),
  SUM(size),
  COUNT(date),
  COUNT(feepct),
  MIN(LENGTH(feepct)),
  MAX(LENGTH(feepct)),
  COUNT(country),
  MIN(LENGTH(country)),
  MAX(LENGTH(country)) 
FROM "public"."demodata"
  | SQL took 0:00:00.004577
  | Query measurement: row_count = 67
  | Query measurement: values_count(id) = 67
  | Query measurement: min_length(id) = 36
  | Query measurement: max_length(id) = 36
  | Query measurement: values_count(name) = 67
  | Query measurement: min_length(name) = 9
  | Query measurement: max_length(name) = 19
  | Query measurement: values_count(size) = 67
  | Query measurement: min(size) = 1126
  | Query measurement: max(size) = 9894
  | Query measurement: avg(size) = 5773.1343283582089552
  | Query measurement: sum(size) = 386800
  | Query measurement: values_count(date) = 67
  | Query measurement: values_count(feepct) = 67
  | Query measurement: min_length(feepct) = 7
  | Query measurement: max_length(feepct) = 7
  | Query measurement: values_count(country) = 67
  | Query measurement: min_length(country) = 2
  | Query measurement: max_length(country) = 11
  | Derived measurement: missing_percentage(id) = 0.0
  | Derived measurement: missing_count(id) = 0
  | Derived measurement: values_percentage(id) = 100.0
  | Derived measurement: missing_percentage(name) = 0.0
  | Derived measurement: missing_count(name) = 0
  | Derived measurement: values_percentage(name) = 100.0
  | Derived measurement: missing_percentage(size) = 0.0
  | Derived measurement: missing_count(size) = 0
  | Derived measurement: values_percentage(size) = 100.0
  | Derived measurement: missing_percentage(date) = 0.0
  | Derived measurement: missing_count(date) = 0
  | Derived measurement: values_percentage(date) = 100.0
  | Derived measurement: missing_percentage(feepct) = 0.0
  | Derived measurement: missing_count(feepct) = 0
  | Derived measurement: values_percentage(feepct) = 100.0
  | Derived measurement: missing_percentage(country) = 0.0
  | Derived measurement: missing_count(country) = 0
  | Derived measurement: values_percentage(country) = 100.0
  | schema = id character varying, name character varying, size integer, date date, feepct character varying, country character varying
  | row_count = 67
  | values_count(id) = 67
  | min_length(id) = 36
  | max_length(id) = 36
  | values_count(name) = 67
  | min_length(name) = 9
  | max_length(name) = 19
  | values_count(size) = 67
  | min(size) = 1126
  | max(size) = 9894
  | avg(size) = 5773.1343283582089552
  | sum(size) = 386800
  | values_count(date) = 67
  | values_count(feepct) = 67
  | min_length(feepct) = 7
  | max_length(feepct) = 7
  | values_count(country) = 67
  | min_length(country) = 2
  | max_length(country) = 11
  | missing_percentage(id) = 0.0
  | missing_count(id) = 0
  | values_percentage(id) = 100.0
  | missing_percentage(name) = 0.0
  | missing_count(name) = 0
  | values_percentage(name) = 100.0
  | missing_percentage(size) = 0.0
  | missing_count(size) = 0
  | values_percentage(size) = 100.0
  | missing_percentage(date) = 0.0
  | missing_count(date) = 0
  | values_percentage(date) = 100.0
  | missing_percentage(feepct) = 0.0
  | missing_count(feepct) = 0
  | values_percentage(feepct) = 100.0
  | missing_percentage(country) = 0.0
  | missing_count(country) = 0
  | values_percentage(country) = 100.0
  | All good. 38 measurements computed. No tests failed.
```

#### 6\) Add a test

See [Tests](tests.md) and add a test to a generated soda.yml files. 

TODO explain this a bit more :)
