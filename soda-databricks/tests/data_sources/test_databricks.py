import os

import pytest
from helpers.test_connection import TestConnection

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "unity_catalog")


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_connection",
        connection_yaml_str=f"""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: {DATABRICKS_HOST}
                    http_path: {DATABRICKS_HTTP_PATH}
                    access_token: {DATABRICKS_TOKEN}
                    catalog: {DATABRICKS_CATALOG}
            """,
    ),
    TestConnection(  # confirm session configuration is applied
        test_name="applies_session_configuration",
        connection_yaml_str=f"""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: {DATABRICKS_HOST}
                    http_path: {DATABRICKS_HTTP_PATH}
                    access_token: {DATABRICKS_TOKEN}
                    catalog: {DATABRICKS_CATALOG}
                    session_configuration: {{"foo":"bar"}}
            """,
        query_should_succeed=False,
        expected_query_error="Configuration foo is not available.",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_databricks_connections(test_connection: TestConnection):
    test_connection.test()
