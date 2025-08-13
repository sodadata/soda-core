import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR", None)
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")

# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_aws_creds",
        connection_yaml_str=f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    staging_dir: {ATHENA_S3_TEST_DIR}
                    region_name: {ATHENA_REGION_NAME}
            """,
    )
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_athena_connections(test_connection: TestConnection, monkeypatch: pytest.MonkeyPatch):
    test_connection.test(monkeypatch=monkeypatch)
