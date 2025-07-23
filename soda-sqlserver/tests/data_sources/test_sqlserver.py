import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "localhost")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE", "master")
SQLSERVER_USERNAME = os.getenv("SQLSERVER_USERNAME", "SA")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD", "Password1!")
SQLSERVER_DRIVER = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
SQLSERVER_TRUST_SERVER_CERTIFICATE = os.getenv("SQLSERVER_TRUST_SERVER_CERTIFICATE", "true")


test_connections: list[TestConnection] = [
    TestConnection(
        test_name="correct_password_auth",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    host: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: '{SQLSERVER_PASSWORD}'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
    ),
    TestConnection(
        test_name="no_server_certificate",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    host: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: '{SQLSERVER_PASSWORD}'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: 'false'
            """,
        valid_connection_params=False,
        expected_connection_error="certificate verify failed",
    ),
    TestConnection(
        test_name="invalid_driver",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    host: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: '{SQLSERVER_PASSWORD}'
                    driver: 'IM_NO_DRIVER'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
        valid_connection_params=False,
        expected_connection_error="[Driver Manager]Can't open lib",
    ),
    TestConnection(
        test_name="incorrect_parameter_hostname",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    hostttt: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: '{SQLSERVER_PASSWORD}'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for SQLServerDataSource",
    ),
    TestConnection(
        test_name="missing_hostname",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: '{SQLSERVER_PASSWORD}'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for SQLServerDataSource",
    ),
    TestConnection(
        test_name="wrong_password",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    host: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    password: 'WRONG_PASSWORD'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
        valid_connection_params=False,
        expected_connection_error="Login failed for user '",
    ),
    TestConnection(
        test_name="activedirectoryserviceprincipal",
        connection_yaml_str=f"""
                type: sqlserver
                name: SQLSERVER_TEST_DS
                connection:
                    host: '{SQLSERVER_HOST}'
                    port: '{SQLSERVER_PORT}'
                    database: '{SQLSERVER_DATABASE}'
                    user: '{SQLSERVER_USERNAME}'
                    client_id: 'some_id'
                    client_secret: 'some_secret'
                    authentication: 'activedirectoryserviceprincipal'
                    driver: '{SQLSERVER_DRIVER}'
                    trust_server_certificate: '{SQLSERVER_TRUST_SERVER_CERTIFICATE}'
            """,
        valid_connection_params=False,
        expected_connection_error="Login failed for user '",
    ),
]


@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_sqlserver_connections(test_connection: TestConnection):
    test_connection.test()
