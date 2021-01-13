# 5 min tutorial

If at any time during this tutorial you get stuck, speak up 
in the [getting-started Slack channel](slack://channel?id=C01HYL8V64C&team=T01HBMYM59V) or 
[post an issue on GitHub](https://github.com/sodadata/soda-sql/issues/new)

Apart from the `soda` CLI, this tutorial also uses  [Docker](https://docs.docker.com/get-docker/) 
to launch a warehouse containing the data to test. If you already have a postgres 
running on your machine, feel free to use that one but make sure you'll use the correct connection 
details.

### 1\) Check your `soda` installation 

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

If you don't get this output, check out [Installation](installation.md)

### 2) Set up an example warehouse 

#### 2.1) Start postgres as your warehouse

This postgres will act as your data warehouse during this tutorial.

If you choose to use your own postgres, make sure you have a sodasql database 
and a sodasql user that does not require a password.   

This next script will place the postgres files in your `~/soda_sql_tutorial_postgres/`
Feel free to use a different location.  

```shell script
$ docker run --name soda_sql_tutorial_db --rm -d \
    -p 5432:5432 \
    -v soda_sql_tutorial_postgres:/var/lib/postgresql/data:rw \
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

After you're done with the tutorial, you can stop the container with 
`docker stop soda_sql_tutorial_db`.  That will also automatically remove the container.

#### 2.2\) Load example data in your warehouse

Next use this command to load [example data](https://github.com/sodadata/soda-sql/blob/main/tests/demo/demodata.sql) 
in your tutorial postgres database.

```
$ docker exec soda_sql_tutorial_db \
  sh -c "wget -qO - https://raw.githubusercontent.com/sodadata/soda-sql/main/tests/demo/demodata.sql | psql -U sodasql -d sodasql"
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

Run `soda create` CLI command to create a warehouse configuration file

The `soda create` will only create and append config files.  It will 
never overwrite or delete things in existing files. So you should not be scared 
it will delete things.  The command reports what it finds and does.

Imagine that you're working on a customers postgres database. 

```
$ soda create -d sodasql -u sodasql ~/soda_sql_tutorial postgres
  | Soda CLI version 2.0.0 beta
  | Creating warehouse directory /Users/tom/soda_sql_tutorial ...
  | Creating warehouse configuration file /Users/tom/soda_sql_tutorial/warehouse.yml ...
  | Creating /Users/tom/.soda/env_vars.yml with example env vars in section soda_sql_tutorial
  | Review warehouse.yml by running command
  |   open /Users/tom/soda_sql_tutorial/warehouse.yml
  | Review section soda_sql_tutorial in ~/.soda/env_vars.yml by running command
  |   open ~/.soda/env_vars.yml
  | Then run
  |   soda init /Users/tom/soda_sql_tutorial
```

To learn more on this command, see [soda create](cli.md#create) 

Next, review the 2 files that have been created: 
 * `open ~/soda_sql_tutorial/warehouse.yml`
 * `open ~/.soda/env_vars.yml`
 
You can continue without changing anything.

### 4\) Initialize table scan.yml files 

Use the `init` helps to create a `scan.yml` for each table in your warehouse
with good defaults that you can customize.

```
$ soda init ~/soda_sql_tutorial
  | Soda CLI version 2.0.0 beta
  | Initializing /Users/tom/soda_sql_tutorial ...
  | Querying warehouse for tables
  | Executing SQL query: 
SELECT table_name 
FROM information_schema.tables 
WHERE lower(table_schema)='public'
  | SQL took 0:00:00.005413
  | Creating table directory /Users/tom/soda_sql_tutorial/demodata
  | Creating /Users/tom/soda_sql_tutorial/demodata/scan.yml ...
  | Next run 'soda scan /Users/tom/soda_sql_tutorial demodata' to calculate measurements and run tests
```

### 5\) Review the generated scan.yaml files

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
tests:
  rows: row_count > 0
```

### 6\) Run a scan 

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
...
  | missing_count(country) = 0
  | values_percentage(country) = 100.0
  | All good. 38 measurements computed. No tests failed.
```

### 7\) Next steps

* See [Tests](tests.md) to add tests.
* See [SQL Metrics](sql_metrics.md) to add a custom SQL query as your metric.
* See [Orchestrate scans](orchestrate_scans.md) to add scans to your data pipeline.
