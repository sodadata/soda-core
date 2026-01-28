import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR")
# Drop the extra / at the end of the S3 test dir. This gives conflicts with the location to clean up later.
ATHENA_S3_TEST_DIR = ATHENA_S3_TEST_DIR[:-1] if ATHENA_S3_TEST_DIR.endswith("/") else ATHENA_S3_TEST_DIR
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")
ATHENA_CATALOG = os.getenv("ATHENA_CATALOG", "awsdatacatalog")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP")

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
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """,
    ),
    TestConnection(
        test_name="missing_credentials",
        connection_yaml_str=f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_keyssdfasdf: some_secret
                    staging_dir: {ATHENA_S3_TEST_DIR}
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required [type=missing,",
    ),
    TestConnection(
        test_name="incorrect_secret_access_key",
        connection_yaml_str=f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: WRONG_SECRET
                    staging_dir: {ATHENA_S3_TEST_DIR}
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """,
        query_should_succeed=False,
        expected_query_error="An error occurred (InvalidSignatureException)",
    ),
    TestConnection(
        test_name="missing_staging_dir",
        connection_yaml_str=f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
                    {f"work_group: {ATHENA_WORKGROUP}" if ATHENA_WORKGROUP else ""}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required [type=missing, ",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_athena_connections(test_connection: TestConnection, monkeypatch: pytest.MonkeyPatch):
    test_connection.test(monkeypatch=monkeypatch)
