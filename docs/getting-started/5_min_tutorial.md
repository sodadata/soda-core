---
layout: default
title: 5 min tutorial
parent: Getting Started
nav_order: 2
---

# 5 min tutorial

If at any time during this tutorial you get stuck, speak up
in our [GitHub Discussions forum](https://github.com/sodadata/soda-sql/discussions/).

### 1\) Check your CLI installation

Open a command line and enter `soda` to verify if the soda-sql command line tool is installed correctly.

If you don't get this kind of output, check out our [Installation guide]({% link getting-started/installation.md %}).

```shell
$ soda
Usage: soda [OPTIONS] COMMAND [ARGS]...
...
```



### 2) Set up an example warehouse

In this tutorial we'll use PostgreSQL as our data warehouse. Note that Soda SQL also supports
Snowflake, AWS Athena, GCP BigQuery, AWS Redshift and others.

#### 2.1) Start PostgreSQL as your warehouse

To get you going we've included the steps required to setup a pre-configured PostgreSQL container,
but you can also choose to use your own PostgreSQL installation. If so, make sure to create
a `sodasql` database and an associated `sodasql` user which doesn't require a password.

_Command:_
```shell
$ docker run --name soda_sql_tutorial_db --rm -d \
    -p 5432:5432 \
    -v soda_sql_tutorial_postgres:/var/lib/postgresql/data:rw \
    -e POSTGRES_USER=sodasql \
    -e POSTGRES_DB=sodasql \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    postgres:9.6.17-alpine
```

> As soon as you're done with the tutorial you can use the following commands to clean up the created container and volume:
> ```shell
> $ docker stop soda_sql_tutorial_db
> $ docker volume rm soda_sql_tutorial_postgres
> ```

#### 2.2\) Load example data into your warehouse

Use the following command to load [example data](https://github.com/sodadata/soda-sql/blob/main/tests/demo/demodata.sql)
into your PostgreSQL tutorial database:

_Command:_
```shell
docker exec soda_sql_tutorial_db \
  sh -c "wget -qO - https://raw.githubusercontent.com/sodadata/soda-sql/main/tests/demo/demodata.sql | psql -U sodasql -d sodasql"
```
_Command console output:_
```shell
DROP TABLE
CREATE TABLE
INSERT 0 6
INSERT 0 8
INSERT 0 9
INSERT 0 8
INSERT 0 10
INSERT 0 12
INSERT 0 12
```

### 3\) Create a warehouse directory

With our database up-and-running it's time to create our warehouse configuration. In this tutorial we will name our
warehouse directory `soda_sql_tutorial` and we'll use the `soda` CLI tool to create the initial
directory and `warehouse.yml`.  The `warehouse.yml` file which
will be created by the command below will include connection details to use the PostgreSQL
database we've just set up.  The command will also create and store the credentials in
`~/.soda/env_vars.yml`

_Command:_
```shell
soda create -d sodasql -u sodasql -w postgres_local_tutorial postgres
```
_Command console output:_
```
  | Soda CLI version 
  | Creating warehouse YAML file warehouse.yml ...
  | Creating /Users/tom/.soda/env_vars.yml with example env vars in section postgres_local_tutorial
  | Review warehouse.yml by running command
  |   cat warehouse.yml
  | Review section postgres_local_tutorial in ~/.soda/env_vars.yml by running command
  |   cat ~/.soda/env_vars.yml
  | Then run the soda init command
```

The `soda create` command will only create and append configuration files.  It will
never overwrite or delete existing files so you can safely run the command
multiple times, or against an existing directory.

Next, review the 2 files that have been created:
 * `cat ./warehouse.yml`
 * `cat ~/.soda/env_vars.yml`

You can continue without changing anything.

Check out the [warehouse.yml]({% link documentation/warehouse.md %}) or [env_vars.yml]({% link documentation/cli.md %}#env-vars) documentation to learn more about these files.

### 4\) Initialize table scan YAML files

Now our warehouse is configured it's time to create a scan YAML file for the tables we want to scan.
We can run the `soda init` command to automatically generate a `{table_name}.yml` for each table
in our PostgreSQL warehouse:

The `soda init` will by default use `warehouse.yml` in the current directory.  Run this command in 
the directory where the `warehouse.yml` is located that was just created in the previous step:

_Command:_
```shell
soda init 
```
_Command console output:_
```
soda init
  | 
  | Initializing warehouse.yml ...
  | Querying warehouse for tables
  | Creating tables directory tables
  | Executing SQL query: 
SELECT table_name 
FROM information_schema.tables 
WHERE lower(table_schema)='public'
  | SQL took 0:00:00.011542
  | Creating tables/demodata.yml ...
  ... Some queries will be executed and shown on the console.  The queries are used   ... 
  ... to do type inference and prepare default scan YAML files that you can customize ... 
  | Next run 'soda scan tables/demodata.yml' to calculate measurements and run tests
```

### 5\) Review the generated scan YAML files

Each scan YAML file will contain the metric and test instructions used by `soda scan`. By default `soda init` will
create a scan YAML file with some good defaults, but feel free to modify the generated configurations
to fit your needs.

> Head over to the [Scan Documentation]({% link documentation/scan.md %}) for more in-depth information about scan YAML files.

_Command:_
```shell
cat ./tables/demodata.yml
```
_Command console output:_
```yaml
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
  - min_length
  - max_length
  - avg_length
  - min
  - max
  - avg
  - sum
  - variance
  - stddev
tests:
  - row_count > 0
columns:
  id:
    valid_format: uuid
    tests:
      - invalid_percentage == 0
  feepct:
    valid_format: number_percentage
    tests:
      - invalid_percentage == 0
```

### 6\) Run a scan

With your warehouse directory created and initialized it's time to start scanning. 

Each scan requires a warehouse YAML and a scan YAML as input.  The scan command will collect the configured 
metrics and run the defined tests against them.

To run your first scan on the `demodata` table simply run:

_Command:_
```shell
soda scan warehouse.yml tables/demodata.yml
```
_Command console output:_
```shell
  | Soda CLI version 2.0.0 beta
  | Scanning demodata in ./soda_sql_tutorial ...
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
...
  | missing_count(country) = 0
  | values_percentage(country) = 100.0
  | All good. 38 measurements computed. No tests failed.
```

### 7\) Next steps

Congrats! You've just completed all steps required to get you going with `soda-sql`.

[Post a quick note letting us know what you like or dislike.](https://github.com/sodadata/soda-sql/discussions/new)

Next we suggest you to take a look at some further in-depth documentation which will help you to integrate `soda-sql` into
your own project.

* See [Tests]({% link documentation/tests.md %}) to add tests.
* See [SQL Metrics]({% link documentation/sql_metrics.md %}) to add a custom SQL query as your metric.
* See [Orchestrate scans]({% link documentation/orchestrate_scans.md %}) to add scans to your data pipeline.