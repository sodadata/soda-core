import os

import pytest
from helpers.test_connection import TestConnection

FABRIC_HOST = os.getenv("FABRIC_HOST", "localhost")
FABRIC_PORT = os.getenv("FABRIC_PORT", "1433")
FABRIC_DATABASE = os.getenv("FABRIC_DATABASE", "soda-ci-fabric-warehouse")
FABRIC_AUTHENTICATION_TYPE = os.getenv("FABRIC_AUTHENTICATION_TYPE", "activedirectoryserviceprincipal")
FABRIC_CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
FABRIC_CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
FABRIC_DRIVER = os.getenv("FABRIC_DRIVER", "ODBC Driver 18 for SQL Server")

test_connections: list[TestConnection] = [
    TestConnection(
        test_name="correct_connection",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                host: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{FABRIC_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="no_server_certificate",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                host: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: false
                driver: '{FABRIC_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="invalid_driver",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                host: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: 'IM_NO_DRIVER'
                autocommit: true
            """,
        valid_connection_params=False,
        expected_connection_error="[Driver Manager]Can't open lib",
    ),
    TestConnection(
        test_name="incorrect_parameter_hostname",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                hostttt: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{FABRIC_DRIVER}'
                autocommit: true
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for FabricDataSource",
    ),
    TestConnection(
        test_name="missing_hostname",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{FABRIC_DRIVER}'
                autocommit: true
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for FabricDataSource",
    ),
    TestConnection(
        test_name="authentication_type_missing",  # Defaults to the correct one
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                host: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                client_id: '{FABRIC_CLIENT_ID}'
                client_secret: '{FABRIC_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{FABRIC_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="wrong_client_id_and_secret",
        connection_yaml_str=f"""
            type: fabric
            name: FABRIC_TEST_DS
            connection:
                host: '{FABRIC_HOST}'
                port: '{FABRIC_PORT}'
                database: '{FABRIC_DATABASE}'
                authentication: '{FABRIC_AUTHENTICATION_TYPE}'
                client_id: 'WRONG_CLIENT_ID'
                client_secret: 'WRONG_CLIENT_SECRET'
                trust_server_certificate: true
                driver: '{FABRIC_DRIVER}'
                autocommit: true
                login_timeout: 2
            """,
        valid_connection_params=False,
        expected_connection_error="Login timeout expired (0)",
    ),
]


@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_FABRIC_connections(test_connection: TestConnection):
    test_connection.test()
