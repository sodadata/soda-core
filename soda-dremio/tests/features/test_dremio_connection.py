import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
DREMIO_HOST = os.getenv("DREMIO_HOST", "localhost")
DREMIO_USERNAME = os.getenv("DREMIO_USERNAME", "admin")
DREMIO_PASSWORD = os.getenv("DREMIO_PASSWORD", "admin1234")
DREMIO_PORT = os.getenv("DREMIO_PORT", "32010")


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_connection",
        connection_yaml_str=f"""
                type: dremio
                name: DREMIO_TEST
                connection:
                    host: '{DREMIO_HOST}'
                    username: '{DREMIO_USERNAME}'
                    password: '{DREMIO_PASSWORD}'
                    port: '{DREMIO_PORT}'
            """,
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_dremio_connections(test_connection: TestConnection):
    test_connection.test()
