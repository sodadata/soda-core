import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
ODPS_ACCESS_ID = os.getenv("ODPS_ACCESS_ID", None)
ODPS_SECRET_ACCESS_KEY = os.getenv("ODPS_SECRET_ACCESS_KEY", None)
ODPS_PROJECT = os.getenv("ODPS_PROJECT", None)
ODPS_ENDPOINT = os.getenv("ODPS_ENDPOINT", "https://service.odps.aliyun.com/api")
ODPS_TUNNEL_ENDPOINT = os.getenv("ODPS_TUNNEL_ENDPOINT", None)

# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_odps_creds",
        connection_yaml_str=f"""
                type: odps
                name: ODPS_TEST_DS
                connection:
                    access_id: {ODPS_ACCESS_ID}
                    secret_access_key: {ODPS_SECRET_ACCESS_KEY}
                    project: {ODPS_PROJECT}
                    endpoint: {ODPS_ENDPOINT}
                    {f"tunnel_endpoint: {ODPS_TUNNEL_ENDPOINT}" if ODPS_TUNNEL_ENDPOINT else ""}
            """,
    ),
    TestConnection(
        test_name="missing_credentials",
        connection_yaml_str=f"""
                type: odps
                name: ODPS_TEST_DS
                connection:
                    access_id: {ODPS_ACCESS_ID}
                    secret_access_keyssdfasdf: some_secret
                    project: {ODPS_PROJECT}
                    endpoint: {ODPS_ENDPOINT}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required [type=missing]",
    ),
    TestConnection(
        test_name="missing_project",
        connection_yaml_str=f"""
                type: odps
                name: ODPS_TEST_DS
                connection:
                    access_id: {ODPS_ACCESS_ID}
                    secret_access_key: {ODPS_SECRET_ACCESS_KEY}
                    endpoint: {ODPS_ENDPOINT}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required [type=missing",
    ),
    TestConnection(
        test_name="missing_endpoint",
        connection_yaml_str=f"""
                type: odps
                name: ODPS_TEST_DS
                connection:
                    access_id: {ODPS_ACCESS_ID}
                    secret_access_key: {ODPS_SECRET_ACCESS_KEY}
                    project: {ODPS_PROJECT}
            """,
        valid_yaml=False,
        expected_yaml_error="Field required [type=missing",
    ),
]


# run tests. parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_odps_connections(test_connection: TestConnection, monkeypatch: pytest.MonkeyPatch):
    test_connection.test(monkeypatch=monkeypatch)
