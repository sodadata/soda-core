import os

import pytest
from helpers.test_connection import TestConnection

SYNAPSE_HOST = os.getenv("SYNAPSE_HOST", "localhost")
SYNAPSE_PORT = os.getenv("SYNAPSE_PORT", "1433")
SYNAPSE_DATABASE = os.getenv("SYNAPSE_DATABASE", "sodatestingsynapse")
SYNAPSE_AUTHENTICATION_TYPE = os.getenv("SYNAPSE_AUTHENTICATION_TYPE", "activedirectoryserviceprincipal")
SYNAPSE_CLIENT_ID = os.getenv("SYNAPSE_CLIENT_ID")
SYNAPSE_CLIENT_SECRET = os.getenv("SYNAPSE_CLIENT_SECRET")
SYNAPSE_DRIVER = os.getenv("SYNAPSE_DRIVER", "ODBC Driver 18 for SQL Server")

test_connections: list[TestConnection] = [
    TestConnection(
        test_name="correct_connection",
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="no_server_certificate",
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
                trust_server_certificate: false
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="invalid_driver",
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
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
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                hostttt: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for SynapseDataSource",
    ),
    TestConnection(
        test_name="missing_hostname",
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
            """,
        valid_yaml=False,
        expected_yaml_error="validation errors for SynapseDataSource",
    ),
    TestConnection(
        test_name="authentication_type_missing",  # Defaults to the correct one
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                client_id: '{SYNAPSE_CLIENT_ID}'
                client_secret: '{SYNAPSE_CLIENT_SECRET}'
                trust_server_certificate: true
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
            """,
    ),
    TestConnection(
        test_name="wrong_client_id_and_secret",
        connection_yaml_str=f"""
            type: synapse
            name: SYNAPSE_TEST_DS
            connection:
                host: '{SYNAPSE_HOST}'
                port: '{SYNAPSE_PORT}'
                database: '{SYNAPSE_DATABASE}'
                authentication: '{SYNAPSE_AUTHENTICATION_TYPE}'
                client_id: 'WRONG_CLIENT_ID'
                client_secret: 'WRONG_CLIENT_SECRET'
                trust_server_certificate: true
                driver: '{SYNAPSE_DRIVER}'
                autocommit: true
                login_timeout: 2
            """,
        valid_connection_params=False,
        expected_connection_error="Login timeout expired (0)",
    ),
]


@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_sqlserver_connections(test_connection: TestConnection):
    test_connection.test()
