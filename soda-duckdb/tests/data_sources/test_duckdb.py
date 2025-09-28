import os

import pandas as pd
import polars as pl
import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_connection import TestConnection
from helpers.test_table import TestTableSpecification

# Teset table spec is just used to build the contract, the actual data is in the df/csv/etc
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count_duckdb")
    .column_varchar("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)

contract_str = f"""
    columns:
        - name: id
          data_type: text
          checks:
            - missing:
    checks:
        - row_count:
            threshold:
                must_be: 3
"""


def test_df(data_source_test_helper: DataSourceTestHelper):
    test_df = pd.DataFrame.from_dict({"id": [1, 2, 3]})

    test_table = data_source_test_helper._create_test_table_python_object(test_table_specification)

    con = data_source_test_helper.data_source_impl.connection.connection
    con.register(test_table.unique_name, test_df)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )


def test_polars(data_source_test_helper: DataSourceTestHelper):
    test_df = pl.DataFrame({"id": [1, 2, 3]})

    test_table = data_source_test_helper._create_test_table_python_object(test_table_specification)

    con = data_source_test_helper.data_source_impl.connection.connection
    con.register(test_table.unique_name, test_df)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )


DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "unity_catalog")


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="standard_connection",
        connection_yaml_str=f"""
                type: duckdb
                name: DUCKDB_TEST
                connection:
                    database: ":memory:"
            """,
    ),
    # TestConnection(  # test read_only param is applied
    #     test_name="read_only_applied",
    #     connection_yaml_str=f"""
    #             type: duckdb
    #             name: DUCKDB_TEST
    #             connection:
    #                 database: ":memory:"
    #                 read_only: true
    #         """,
    #     valid_connection_params=False,
    #     expected_error_message="Cannot launch in-memory database in read-only mode"
    # ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_duckdb_connections(test_connection: TestConnection):
    test_connection.test()
