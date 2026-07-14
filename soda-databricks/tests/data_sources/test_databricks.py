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
# OAuth service-principal creds (client-credentials M2M)
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_CLIENT_SECRET = os.getenv("DATABRICKS_CLIENT_SECRET")
# Azure Entra ID service-principal creds
DATABRICKS_AZURE_CLIENT_ID = os.getenv("DATABRICKS_AZURE_CLIENT_ID")
DATABRICKS_AZURE_CLIENT_SECRET = os.getenv("DATABRICKS_AZURE_CLIENT_SECRET")
DATABRICKS_AZURE_TENANT_ID = os.getenv("DATABRICKS_AZURE_TENANT_ID")


def _with_https(host: str | None) -> str | None:
    if host and "://" not in host:
        return f"https://{host}"
    return host


DATABRICKS_HOSTNAME_WITH_HTTPS = _with_https(DATABRICKS_HOST)

# Which live-credential sets are available. Live cases are only added when their creds are
# present, so the module imports and the no-credential (config-validation) cases below still
# run in any environment.
_has_pat = all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN])
_has_oauth_m2m = all([DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET])
_has_azure_sp = all(
    [
        DATABRICKS_HOST,
        DATABRICKS_HTTP_PATH,
        DATABRICKS_AZURE_CLIENT_ID,
        DATABRICKS_AZURE_CLIENT_SECRET,
        DATABRICKS_AZURE_TENANT_ID,
    ]
)

# define test cases and expected behavior (passing unless otherwise specified)
# Config-validation cases run everywhere (no creds/live connection): they confirm auth_type
# dispatch and required-field validation through the full YAML -> DataSourceImpl path.
test_connections: list[TestConnection] = [
    TestConnection(  # unknown discriminator must be rejected at parse time
        test_name="unknown_auth_type_rejected",
        connection_yaml_str="""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: abc.cloud.databricks.com
                    http_path: /sql/1.0/endpoints/abc
                    auth_type: not-a-real-mode
            """,
        valid_yaml=False,
        expected_yaml_error="Unknown Databricks auth_type",
    ),
    TestConnection(  # OAuth M2M without its required secret must be rejected at parse time
        test_name="oauth_m2m_missing_client_secret_rejected",
        connection_yaml_str="""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: abc.cloud.databricks.com
                    http_path: /sql/1.0/endpoints/abc
                    auth_type: databricks-oauth-m2m
                    client_id: some-client-id
            """,
        valid_yaml=False,
        expected_yaml_error="client_secret",
    ),
    TestConnection(  # Azure service principal without its tenant id must be rejected at parse time
        test_name="azure_service_principal_missing_tenant_rejected",
        connection_yaml_str="""
                type: databricks
                name: DATABRICKS_TEST
                connection:
                    host: adb-123.azuredatabricks.net
                    http_path: /sql/1.0/endpoints/abc
                    auth_type: azure-service-principal
                    azure_client_id: some-client-id
                    azure_client_secret: some-secret
            """,
        valid_yaml=False,
        expected_yaml_error="azure_tenant_id",
    ),
]

if _has_pat:
    test_connections += [
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

if _has_oauth_m2m:
    test_connections.append(
        TestConnection(  # live Databricks-managed OAuth M2M (service principal)
            test_name="oauth_m2m_connection",
            connection_yaml_str=f"""
                    type: databricks
                    name: DATABRICKS_TEST
                    connection:
                        host: {DATABRICKS_HOST}
                        http_path: {DATABRICKS_HTTP_PATH}
                        auth_type: databricks-oauth-m2m
                        client_id: {DATABRICKS_CLIENT_ID}
                        client_secret: {DATABRICKS_CLIENT_SECRET}
                        catalog: {DATABRICKS_CATALOG}
                """,
        )
    )

if _has_azure_sp:
    test_connections.append(
        TestConnection(  # live Azure Entra ID service principal
            test_name="azure_service_principal_connection",
            connection_yaml_str=f"""
                    type: databricks
                    name: DATABRICKS_TEST
                    connection:
                        host: {DATABRICKS_HOST}
                        http_path: {DATABRICKS_HTTP_PATH}
                        auth_type: azure-service-principal
                        azure_client_id: {DATABRICKS_AZURE_CLIENT_ID}
                        azure_client_secret: {DATABRICKS_AZURE_CLIENT_SECRET}
                        azure_tenant_id: {DATABRICKS_AZURE_TENANT_ID}
                        catalog: {DATABRICKS_CATALOG}
                """,
        )
    )


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
