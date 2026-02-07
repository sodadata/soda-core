import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
TRINO_HOST = os.getenv("TRINO_HOST", "")
TRINO_USERNAME = os.getenv("TRINO_USERNAME", "")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "")




# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_username_password",
        connection_yaml_str=f"""
                type: trino
                name: TRINO_TEST_DS
                connection:
                    host: '{TRINO_HOST}'
                    username: {TRINO_USERNAME}
                    password: '{TRINO_PASSWORD}'
                    catalog: '{TRINO_CATALOG}'
            """,
    )
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_trino_connections(test_connection: TestConnection, monkeypatch: pytest.MonkeyPatch):
    test_connection.test(monkeypatch=monkeypatch)
