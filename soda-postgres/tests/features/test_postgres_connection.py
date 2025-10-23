import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME", "soda_test")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "***")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "soda_test")

with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file.write(POSTGRES_PASSWORD.encode())
    POSTGRES_PASSWORD_FILE_PATH = temp_file.name


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_connection",
        connection_yaml_str=f"""
                type: postgres
                name: POSTGRES_TEST
                connection:
                    host: '{POSTGRES_HOST}'
                    user: '{POSTGRES_USERNAME}'
                    password: '{POSTGRES_PASSWORD}'
                    port: '{POSTGRES_PORT}'
                    database: '{POSTGRES_DATABASE}'
            """,
    ),
    TestConnection(  # use password file, should work
        test_name="correct_connection_password_file",
        connection_yaml_str=f"""
                type: postgres
                name: POSTGRES_TEST
                connection:
                    host: '{POSTGRES_HOST}'
                    user: '{POSTGRES_USERNAME}'
                    password_file: '{POSTGRES_PASSWORD_FILE_PATH}'
                    port: '{POSTGRES_PORT}'
                    database: '{POSTGRES_DATABASE}'
            """,
    ),
    TestConnection(  # attempt ssl connection, should fail with correct error
        test_name="ssl_connect",
        connection_yaml_str=f"""
                type: postgres
                name: POSTGRES_TEST
                connection:
                    host: '{POSTGRES_HOST}'
                    user: '{POSTGRES_USERNAME}'
                    password: '{POSTGRES_PASSWORD}'
                    port: '{POSTGRES_PORT}'
                    database: '{POSTGRES_DATABASE}'
                    sslmode: 'require'
                    sslcert: 'foo'
                    sslkey: 'bar'
                    sslrootcert: 'baz'
            """,
        valid_connection_params=False,
        expected_connection_error="server does not support SSL, but SSL was required",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_postgres_connections(test_connection: TestConnection):
    test_connection.test()
