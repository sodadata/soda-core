import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_json",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    password: '{REDSHIFT_PASSWORD}'
            """,
    )    
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_redshift_connections(test_connection: TestConnection):
    test_connection.test()
