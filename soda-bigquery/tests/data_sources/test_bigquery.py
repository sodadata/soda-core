import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
BIGQUERY_ACCOUNT_INFO_JSON = os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US")
with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file.write(BIGQUERY_ACCOUNT_INFO_JSON.encode())
    BIGQUERY_ACCOUNT_INFO_JSON_PATH = temp_file.name


# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # correct connection, should work
        test_name="correct_json",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    location: '{BIGQUERY_LOCATION}'
            """,
    ),
    TestConnection(  # missing required field, should fail
        test_name="yaml_missing_required",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_jsosn: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    location: '{BIGQUERY_LOCATION}'
            """,
        valid_yaml=False,
        expected_yaml_error="""BigQueryJSONStringAuth.account_info_json\n  Field required [type=missing, input_value={\'account_info_jsosn\'""",
    ),
    TestConnection(  # passing bad credentials, should parse, but fail to connect
        test_name="incorrect_json",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: 'BAD_CREDENTIALS'
                    location: '{BIGQUERY_LOCATION}'
            """,
        valid_connection_params=False,
        expected_connection_error="Could not connect to 'BIGQUERY_TEST_DS'",
    ),
    TestConnection(  # passing bad path, should parse, but fail to connect
        test_name="json_path_issing",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json_path: 'missing.json'
                    location: '{BIGQUERY_LOCATION}'
            """,
        valid_connection_params=False,
        expected_connection_error="No such file or directory",
    ),
    TestConnection(  # passing good path, should work
        test_name="correct_json_path",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json_path: '{BIGQUERY_ACCOUNT_INFO_JSON_PATH}'
                    location: '{BIGQUERY_LOCATION}'
            """,
    ),
    TestConnection(  # use application default credentials, should fail
        # note if you have credentials in your env, you should remove them if you want this test to pass
        # otherwise you'll get a different error and the test will fail
        test_name="app_default_creds",
        connection_yaml_str="""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    use_context_auth: true
            """,
        valid_connection_params=False,
        expected_connection_error="Your default credentials were not found",
    ),
    TestConnection(  # impersonation account, should fail at query stage
        test_name="impersonation_account",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    impersonation_account: 'test@test.com'
            """,
        query_should_succeed=False,
        expected_query_error="Unable to acquire impersonated credentials",
    ),
    TestConnection(  # prove that delegates are propagating correctly
        test_name="impersonation_account_with_invalid_delegates",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    impersonation_account: 'test@test.com'
                    delegates: ['invalid_delegate']
            """,
        query_should_succeed=False,
        expected_query_error="Invalid form of account ID invalid_delegate",
    ),
    TestConnection(  # prove that extra fields are accepted and passed through without error
        test_name="extra_field_accepted",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    extra_field: 'extra_value'
            """,
    ),
    TestConnection(  # prove that custom project_id is being set and used
        test_name="custom_project_id",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    project_id: 'bigquery-public-data'
            """,
        query_should_succeed=False,
        expected_query_error="User does not have bigquery.jobs.create permission in project bigquery-public-data",
    ),
    TestConnection(  # prove that location is being set at all
        test_name="new_invalid_location",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    location: 'XX'
            """,
        query_should_succeed=False,
        expected_query_error="Location XX does not support this operation",
    ),
    TestConnection(  # prove that a valid location is propagating correctly
        test_name="new_valid_location",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    location: 'asia-east1'
            """,
    ),
    TestConnection(  # prove that auth scopes are propagating correctly
        test_name="invalid_auth_scopes",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    auth_scopes: ['invalid']
            """,
        query_should_succeed=False,
        expected_query_error="No access token in response",
    ),
    TestConnection(  # prove that labels are propagating correctly
        test_name="invalid_labels",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    labels: {{'-invalid-': "invalid"}}
            """,
        query_should_succeed=False,
        expected_query_error='Label key "-invalid-" has invalid characters.',
    ),
    TestConnection(  # prove that client options are propagating correctly
        test_name="invalid_client_options",
        connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{BIGQUERY_ACCOUNT_INFO_JSON}'
                    client_options: {{'-invalid-': "invalid"}}
            """,
        valid_connection_params=False,
        expected_connection_error="ClientOptions does not accept an option '-invalid-'",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_bigquery_connections(test_connection: TestConnection):
    test_connection.test()
