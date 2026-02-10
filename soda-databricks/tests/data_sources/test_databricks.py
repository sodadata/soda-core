import os

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_connection import TestConnection
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("1-schema_databricks-special-chars")
    .column_varchar("id-1")
    .column_integer("2-size")
    .column_date("/+created")
    .build()
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "unity_catalog")
DATABRICKS_HOSTNAME_WITH_HTTPS = (
    f"https://{DATABRICKS_HOST}"
    if not (DATABRICKS_HOST.startswith("https://") or DATABRICKS_HOST.startswith("http://"))
    else DATABRICKS_HOST
)

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
    TestConnection(  # correct connection, should work
        test_name="connection_with_https_prefix",
        connection_yaml_str=f"""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: {DATABRICKS_HOSTNAME_WITH_HTTPS}
                    http_path: {DATABRICKS_HTTP_PATH}
                    access_token: {DATABRICKS_TOKEN}
                    catalog: {DATABRICKS_CATALOG}
            """,
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_databricks_connections(test_connection: TestConnection):
    test_connection.test()


def test_databricks_schema_check_special_chars(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id-1
                data_type: {test_table.data_type('id-1')}
              - name: 2-size
                data_type: {test_table.data_type('2-size')}
              - name: /+created
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    schema_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert schema_diagnostics["type"] == "schema"
    assert set([c["name"] for c in schema_diagnostics["actual"]]) == {"id-1", "2-size", "/+created"}
    assert set([c["name"] for c in schema_diagnostics["expected"]]) == {"id-1", "2-size", "/+created"}
