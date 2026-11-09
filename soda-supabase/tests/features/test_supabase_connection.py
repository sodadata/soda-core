import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

SUPABASE_HOST = os.getenv("SUPABASE_HOST", "localhost")
SUPABASE_USERNAME = os.getenv("SUPABASE_USERNAME", "soda_test")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD", "***")
SUPABASE_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_DATABASE = os.getenv("SUPABASE_DATABASE", "soda_test")
SUPABASE_SSLMODE = os.getenv("SUPABASE_SSLMODE", "prefer")

with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file.write(SUPABASE_PASSWORD.encode())
    SUPABASE_PASSWORD_FILE_PATH = temp_file.name


test_connections: list[TestConnection] = [
    TestConnection(
        test_name="correct_connection",
        connection_yaml_str=f"""
                type: supabase
                name: SUPABASE_TEST
                connection:
                    host: '{SUPABASE_HOST}'
                    user: '{SUPABASE_USERNAME}'
                    password: '{SUPABASE_PASSWORD}'
                    port: '{SUPABASE_PORT}'
                    database: '{SUPABASE_DATABASE}'
                    sslmode: '{SUPABASE_SSLMODE}'
            """,
    ),
    TestConnection(
        test_name="correct_connection_password_file",
        connection_yaml_str=f"""
                type: supabase
                name: SUPABASE_TEST
                connection:
                    host: '{SUPABASE_HOST}'
                    user: '{SUPABASE_USERNAME}'
                    password_file: '{SUPABASE_PASSWORD_FILE_PATH}'
                    port: '{SUPABASE_PORT}'
                    database: '{SUPABASE_DATABASE}'
                    sslmode: '{SUPABASE_SSLMODE}'
            """,
    ),
]

if SUPABASE_SSLMODE != "require":
    # The test database has no SSL, so a connection without sslmode proves that require is the default.
    test_connections.append(
        TestConnection(
            test_name="sslmode_defaults_to_require",
            connection_yaml_str=f"""
                type: supabase
                name: SUPABASE_TEST
                connection:
                    host: '{SUPABASE_HOST}'
                    user: '{SUPABASE_USERNAME}'
                    password: '{SUPABASE_PASSWORD}'
                    port: '{SUPABASE_PORT}'
                    database: '{SUPABASE_DATABASE}'
            """,
            valid_connection_params=False,
            expected_connection_error="server does not support SSL, but SSL was required",
        )
    )


@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_supabase_connections(test_connection: TestConnection):
    test_connection.test()
