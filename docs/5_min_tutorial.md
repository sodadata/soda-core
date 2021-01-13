# 5 Minute tutorial

If at any time during this tutorial you get stuck, speak up
in the [getting-started Slack channel](https://soda-community.slack.com/archives/C01HYL8V64C) or
[post an issue on GitHub](https://github.com/sodadata/soda-sql/issues/new).

> This tutorial uses [docker](https://docs.docker.com/get-docker/)
to quickly launch a warehouse containing some test data. Feel free to use a local PostgreSQL
installation, but make sure to use the correct connection details.

### 1\) Check your `soda` installation

Open a command line and enter `soda` to verify if the soda-sql command line tool is installed correctly.

```shell
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

Check out the [Installation Guide](installation.md) if you don't get this output.

### 2) Set up an example warehouse

#### 2.1) Start PostgreSQL as your warehouse

In this tutorial we'll use PostgreSQL as our data warehouse. To get you going we've included
the steps required to setup a pre-configured PostgreSQL container, but you can also choose
to use your own PostgreSQL installation. If so, make sure to create a `sodasql` database
and an associated `sodasql` user which doesn't require a password.

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

#### 2.2\) Load example data in your warehouse

Use the following command to load [example data](https://github.com/sodadata/soda-sql/blob/main/tests/demo/demodata.sql)
into your PostgreSQL tutorial database:

```shell
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

With our database up-and-running it's time to create our warehouse configuration. In your terminal
run the following command to create a warehouse configuration in the `./soda_sql_tutorial` directory.
The warehouse will automatically be configured to use the PostgreSQL database we've just set up.

```shell
$ soda create -d sodasql -u sodasql ./soda_sql_tutorial postgres
  | Soda CLI version 2.0.0 beta
  | Creating warehouse directory ./soda_sql_tutorial ...
  | Creating warehouse configuration file ./soda_sql_tutorial/warehouse.yml ...
  | Creating /Users/tom/.soda/env_vars.yml with example env vars in section soda_sql_tutorial
  | Review warehouse.yml by running command
  |   open ./soda_sql_tutorial/warehouse.yml
  | Review section soda_sql_tutorial in ~/.soda/env_vars.yml by running command
  |   open ~/.soda/env_vars.yml
  | Then run
  |   soda init ./soda_sql_tutorial
```

The `soda create` will only create and append configuration files.  It will
never overwrite or delete existing files so you can safely run the command
multiple times, or against an existing directory.

> Refer to the [CLI Documentation](cli.md) to learn more about the `soda create`
command.

Before you continue we recommend reviewing the 2 files which have been created:
 * `open ./soda_sql_tutorial/warehouse.yml`
 * `open ~/.soda/env_vars.yml`

Check out the [warehouse.yml](warehouse.md) or [env_vars.yml](cli.md?id=env-vars) documentation to learn more about these files.

### 4\) Initialize table scan.yml files

Now our warehouse is configured it's time to initialize it with a `scan.yml` for each table.
To automatically generate a `scan.yml` for each table in our PostgreSQL warehouse we can
run the following command:

```shell
$ soda init ./soda_sql_tutorial
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

### 5\) Review the generated scan.yml files

Each `scan.yml` will contain the metric and test instructions used by `soda scan`. By default `soda init` will
create a `scan.yml` file with some good defaults, but feel free to modify the generated files to fit your needs.

> Head over to the [Scan Documentation](scan.md) for more in-depth information about the `scan.yml` file.

```shell
$ cat ./soda_sql_tutorial/demodata/scan.yml
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

With your warehouse directory created and initialized it's time to start scanning. Each scan
will collect the configured (`scan.yml`) metrics and run the defined tests against them.

To run your first scan on the `demodata` table simply run the following command:

```shell
$ soda scan ./soda_sql_tutorial demodata
  | Soda CLI version 2.0.0 beta
  | Scanning demodata in /Users/tom/soda_sql_tutorial ...
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
[...]
  | missing_count(country) = 0
  | values_percentage(country) = 100.0
  | All good. 38 measurements computed. No tests failed.
```

### 7\) Next steps

That's it! You just completed all steps required to get you going with `soda-sql`. Next we suggest you
to take a look at some further in-depth documentation which will help you to integrate `soda-sql` into
your own project.

* See [Tests](tests.md) to add tests.
* See [SQL Metrics](sql_metrics.md) to add a custom SQL query as your metric.
* See [Orchestrate scans](orchestrate_scans.md) to add scans to your data pipeline.
