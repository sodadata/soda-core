# Overview

Things we need to support a data source:
- Working data source
    - locally - command to launch a docker container with the data source. Docker file or docker compose file.
    - in cloud - service account to connect
- Python library for the connection. This usually means installing an existing official connector library.
- Data source package that handles
    - getting connection properties
    - connecting to the data source
    - any data source specific code to ensure full support

# Implementation

File and folder structure:
- package goes to `soda/xy`, follows same structure as other packages
- main file is `soda/xy/soda/data_sources/xy_data_source.py` with a `XyDataSource(DataSource)` class

Basic code in the Data Source class:
- implement the `__init__` method to retrieve and save connection properties
- implement the `connect` method that returns a PEP 249 compatible connection object

What certainly needs to be overridden:
- type mappings - see more details in the base DataSource class comments.
    - `SCHEMA_CHECK_TYPES_MAPPING`
    - `SQL_TYPE_FOR_CREATE_TABLE_MAP`
    - `SQL_TYPE_FOR_SCHEMA_CHECK_MAP`
    - `NUMERIC_TYPES_FOR_PROFILING`
    - `TEXT_TYPES_FOR_PROFILING`
- `safe_connection_data()` method

What usually needs to be overridden:
- `sql_get_table_names_with_count()` - sql to retrieve all tables with their respective counts. Usually data source specific.
- `default_casify_*()` - indicates any default case manipulation that a data source does when retrieving respective identifiers
- table/column metadata methods:
    - `column_metadata_columns()`
    - `column_metadata_catalog_column()`
    - `sql_get_table_names_with_count()`
- regex support:
    - `escape_regex()` or `escape_string()` to assure correct regex format
    - `regex_replace_flags()` - some data sources support regex replace flags (e.g. `g` for `global`), some do not
- identifier quoting:
    - `quote_*()` methods take care of identifier quoting, `qualified_table_name()` creates a fully qualified table name

What sometimes needs to be overridden:
- any of the `sql_*` methods when a particular data source needs a specific query to get desired result

Things to consider:
- How are schemas (or equivalent) handled - can it be set globally for the connection, or does it need to be prefixed in all the queries?

Tests:
- Create a `soda/xy/tests/text_xy.py` file with `test_xy()` method. This file is used for any data source specific tests.
- Implement `XyDataSourceFixture` for everything tests related:
    - `_build_configuration_dict()` - configuration of the connection used in tests
    - `_create_schema_if_not_exists_sql()` / `_drop_schema_if_exists_sql` - DDL to create / drop a new schema / database

# How to test

- Create `.env` file based on `.env.example` and add appropriate variables for given data source.
- Change the `test_data_source` variable to tested data source
- Run tests using `pytest`
