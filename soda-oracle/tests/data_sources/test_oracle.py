import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
ORACLE_USER = os.getenv("ORACLE_USERNAME")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_CONNECTSTRING = os.getenv("ORACLE_CONNECTSTRING")
ORACLE_HOST = os.getenv("ORACLE_HOST", "localhost")
ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "XE")

# Check if we have the required environment variables for actual connection tests
has_oracle_env_vars = bool(ORACLE_USER and ORACLE_PASSWORD and ORACLE_CONNECTSTRING)
has_oracle_env_vars_for_host_port = bool(ORACLE_USER and ORACLE_PASSWORD)

# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection with connect string, should work
        test_name="correct_connect_string",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                password: '{ORACLE_PASSWORD}'
                connect_string: '{ORACLE_CONNECTSTRING}'
            """,
    ),
    TestConnection(  # correct connection with host/port/service, should work
        test_name="correct_host_port_service",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                password: '{ORACLE_PASSWORD}'
                host: '{ORACLE_HOST}'
                port: {ORACLE_PORT}
                service_name: '{ORACLE_SERVICE_NAME}'
            """,
    ),
    TestConnection(  # missing required field user, should fail
        test_name="yaml_missing_user",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                password: '{ORACLE_PASSWORD}'
                connect_string: '{ORACLE_CONNECTSTRING}'
            """,
        valid_yaml=False,
        expected_yaml_error="Field required",
    ),
    TestConnection(  # missing required field password, should fail
        test_name="yaml_missing_password",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                connect_string: '{ORACLE_CONNECTSTRING}'
            """,
        valid_yaml=False,
        expected_yaml_error="Field required",
    ),
    TestConnection(  # bad credentials, should parse but fail to connect
        test_name="incorrect_credentials",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: 'BAD_USER'
                password: 'BAD_PASSWORD'
                connect_string: '{ORACLE_CONNECTSTRING}'
            """,
        valid_connection_params=False,
        expected_connection_error="Failed to create Oracle connection",
    ),
    TestConnection(  # bad connect string, should parse but fail to connect
        test_name="incorrect_connect_string",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                password: '{ORACLE_PASSWORD}'
                connect_string: 'invalid_host:1521/invalid_service'
            """,
        valid_connection_params=False,
        expected_connection_error="Failed to create Oracle connection",
    ),
    TestConnection(  # bad port number, should fail validation
        test_name="invalid_port",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                password: '{ORACLE_PASSWORD}'
                host: '{ORACLE_HOST}'
                port: 99999
                service_name: '{ORACLE_SERVICE_NAME}'
            """,
        valid_yaml=False,
        expected_yaml_error="Input should be less than or equal to 65535",
    ),
    TestConnection(  # missing service_name in host/port mode, should fail
        test_name="missing_service_name",
        connection_yaml_str=f"""
            type: oracle
            name: ORACLE_TEST_DS
            connection:
                user: '{ORACLE_USER}'
                password: '{ORACLE_PASSWORD}'
                host: '{ORACLE_HOST}'
                port: {ORACLE_PORT}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required",
    )
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_oracle_connections(test_connection: TestConnection):
    # Debug: print environment variable values
    print(f"Debug: ORACLE_USER={repr(ORACLE_USER)}, ORACLE_PASSWORD={repr(ORACLE_PASSWORD)}")
    print(f"Debug: has_oracle_env_vars_for_host_port={has_oracle_env_vars_for_host_port}")
    print(f"Debug: test_name={test_connection.test_name}")
    
    # Skip tests that require actual Oracle connection when env vars are not set
    if test_connection.test_name == "correct_connect_string":
        if not has_oracle_env_vars:
            pytest.skip("Oracle environment variables not set (ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_CONNECTSTRING)")
    elif test_connection.test_name == "correct_host_port_service":
        if not has_oracle_env_vars_for_host_port:
            pytest.skip("Oracle environment variables not set (ORACLE_USERNAME, ORACLE_PASSWORD)")
    
    test_connection.test()
