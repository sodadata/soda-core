# Developer cookbook

This document describes all the procedures that developers need to know to 
contribute effectively to the Soda SQL codebase.

## Scripts

In directory `scripts` there are a number of convenience scripts that also 
serve as documentation for developer recipes.  So you might want to check there 
if you're looking for how to complete a particular procedure.  They are typically 
created on a mac and may not be compatible on other developer systems.

## Getting started with a virtual environment

To get started you need a virtual environment.  Use the following script 
to (re)create a `.venv` virtual environment in the root of the project. 

```
scripts/recreate_venv.sh
```

## Running the pre-commit test suite

Before pushing a commit, be sure to run the pre commit test suite.
The test suite is (and should be) kept fast enough (<2 mins) so that 
it doesn't interrupt the developer flow too much. You can use the script 

```
scripts/run_tests.sh
```

The pre commit test suite requires a local PostgreSQL database 
running with certain user and database preconfigured. Use 
`scripts/start_postgres_container.sh` to start a docker container to 
launch a correct PostgreSQL db with the right user and database.

## Pushing a release

Make sure that you you install dev-requirements
```shell
pip-compile dev-requiremnts.in
pip install -r dev-requiremnts.txt
```

Pushing a release is fully automated and only requires to bump the version using `tbump`. For example to release 2.1.0b3, you can use the following command:

```shell
tbump 2.1.0b3
```

## Adding a new warehouse dialect

Take the following steps to add a warehouse dialect:
1. Create docker container containing a warehouse. For example, 
   [this Docker compose](tests/postgres_container/docker-compose.yml) contains
   the Docker container for the **postgres**.
2. Add the warehouse `config`, `suite` and `fixture` to the [warehouse tests](tests/warehouses)
   - `config`: is the `warehouse.yml` configuration. Name the file
   `<warehouse>_cfg.yml`, for example, the Postgres configuration: 
     [`postgres_cfg.yml`](tests/warehouses/postgres_cfg.yml)
   - `suite`: contains the setup for the warehouse test suite. Name the file
   `<warehouse>_suite.py`, for example, the Postgres suite:
     [`postgres_suite.py`](tests/warehouses/postgres_suite.py)
   - `fixture`: the fixture for the warehouse tests. Name the file
   `<warehouse_fixture.py`, for example, the Postgres fixture:
     [`postgres_fixture.py`](tests/warehouses/postgres_fixture.py)
3. Add the `fixture` to the 
  [general warehouse fixture](tests/common/warehouse_fixture.py). 
4 Add the warehouse dialect under [`packages`](packages):
   - `setup.py` : setup to install the dialect.
   - `sodasql/dialects/<warehouse>_dialect.py`: the [ware house
     dialect](#warehouse-dialect).
  For example the [postgres dialect](packages/postgresql).
5. Add the dialect to [dialect.py](core/sodasql/scan/dialect.py).

### Warehouse dialect

The warehouse dialect defines (i) a couple methods specific to SQL of the dialect
and (ii) the connection to the warehouse. For example the 
[Postgres dialect](packages/postgresql/sodasql/dialect.py).

1. **dialect methods**:
   Amongst others, this includes methods like:
   - `is_text` and `is_number` for detecting the data type
   - `sql_tables_metadata_query` and `sql_columns_metadata_query` for the
     metadata
   - `qualify_table_name` and `qualify_column_name` for the table and column
     name.
   - `sql_expr_` for some sql expressions.
2. **warehouse connection**
   A connection to the warehouse is created with `create_connection`. The
   connection engine should be 
   [Python database API](https://www.python.org/dev/peps/pep-0249/) compatible.
   The connection has two helper method: `is_connection_error`
   and `is_authentication_error`.
