import os

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST", "")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", None)
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE", "soda_test")
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME", "")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD", "")


class MockBoto3Client:
    def __init__(self, *args, **kwargs):
        pass

    def assume_role(self, *args, **kwargs):
        return {
            "Credentials": {"AccessKeyId": "FAKE_KEY", "SecretAccessKey": "FAKE_SECRET", "SessionToken": "FAKE_TOKEN"}
        }

    def get_cluster_credentials(self, *args, **kwargs):
        return {"DbUser": REDSHIFT_USERNAME, "DbPassword": REDSHIFT_PASSWORD}


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_json",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: {REDSHIFT_PORT}
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    password: '{REDSHIFT_PASSWORD}'
            """,
    ),
    TestConnection(  # missing password, should fail
        test_name="missing_password",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'

            """,
        valid_yaml=False,
        expected_yaml_error="password\n  Field required [type=missing,",
    ),
    TestConnection(  # missing user while password is provided, should fail
        test_name="missing_user_with_password",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    password: '{REDSHIFT_PASSWORD}'
            """,
        valid_yaml=False,
        expected_yaml_error="user\n  Field required [type=missing,",
    ),
    TestConnection(  # fake AWS creds (should fail with AWS error)
        test_name="fake_aws_creds",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
            """,
        valid_connection_params=False,
        expected_connection_error="An error occurred (InvalidClientTokenId)",
    ),
    TestConnection(  # missing user while AWS creds are provided, should fail
        test_name="missing_user_with_aws_creds",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
            """,
        valid_yaml=False,
        expected_yaml_error="user\n  Field required [type=missing,",
    ),
    TestConnection(  # correct connection with optional params, should work
        test_name="correct_json_with_params",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    password: '{REDSHIFT_PASSWORD}'
                    keepalives_idle: 10
                    keepalives_interval: 10
                    keepalives_count: 10
                    connect_timeout: 10
            """,
    ),
    TestConnection(  # patched AWS creds (should work)
        test_name="patched_aws_creds",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
            """,
        monkeypatches={"boto3.client": MockBoto3Client},
    ),
    TestConnection(  # patched AWS creds with optional params (should work)
        test_name="patched_aws_creds_with_params",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
                    role_arn: 'FAKE_ROLE_ARN'
                    region: 'FAKE_REGION'
            """,
        monkeypatches={"boto3.client": MockBoto3Client},
    ),
    TestConnection(  # patched AWS creds with cluster identifier (should work)
        test_name="patched_aws_creds_with_cluster_identifier",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '{REDSHIFT_HOST}'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
                    cluster_identifier: 'FAKE_CLUSTER_IDENTIFIER'
            """,
        monkeypatches={"boto3.client": MockBoto3Client},
    ),
    TestConnection(  # patched AWS creds with IP host and no cluster identifier (should fail)
        test_name="patched_aws_creds_with_no_cluster_identifier_and_ip_host",
        connection_yaml_str=f"""
                type: redshift
                name: REDSHIFT_TEST_DS
                connection:
                    host: '127.0.0.1'
                    port: '{REDSHIFT_PORT}'
                    database: '{REDSHIFT_DATABASE}'
                    user: '{REDSHIFT_USERNAME}'
                    access_key_id: 'FAKE_KEY'
                    secret_access_key: 'FAKE_SECRET'
            """,
        monkeypatches={"boto3.client": MockBoto3Client},
        valid_connection_params=False,
        expected_connection_error="Cluster identifier is required when using an IP address as host",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_redshift_connections(test_connection: TestConnection, monkeypatch: pytest.MonkeyPatch):
    test_connection.test(monkeypatch=monkeypatch)
