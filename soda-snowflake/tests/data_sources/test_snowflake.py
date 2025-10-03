import base64
import os
import tempfile

import pytest
from helpers.test_connection import TestConnection

# define environment variables used in test cases
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "soda_test")


PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_ENCRYPTION_KEY")
PRIVATE_KEY = base64.b64decode(os.getenv("SNOWFLAKE_PRIVATE_KEY_ENCRYPTED")).decode("utf-8")
with tempfile.NamedTemporaryFile(delete=False) as temp_file:
    temp_file.write(PRIVATE_KEY.encode())
    PRIVATE_KEY_FILE_PATH = temp_file.name

fake_jwt_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.KMUFsIDTnFmyG3nMiGM6H9FNFUROf3wh7SmqJp-QV30"

# define test cases and expected behavior (passing unless otherwise specified)
test_connections: list[TestConnection] = [
    TestConnection(  # connect using password
        test_name="password_connection",
        connection_yaml_str=f"""
                type: snowflake
                name: SNOWFLAKE_TEST
                connection:
                    account: '{SNOWFLAKE_ACCOUNT}'
                    user: '{SNOWFLAKE_USER}'
                    password: '{SNOWFLAKE_PASSWORD}'
                    database: '{SNOWFLAKE_DATABASE}'
            """,
    ),
    TestConnection(  # connect using private key
        test_name="private_key_connection",
        connection_yaml_str=f"""
                type: snowflake
                name: SNOWFLAKE_TEST
                connection:
                    account: '{SNOWFLAKE_ACCOUNT}'
                    user: '{SNOWFLAKE_USER}'
                    private_key: "{PRIVATE_KEY}"
                    private_key_passphrase: '{PRIVATE_KEY_PASSPHRASE}'
                    database: '{SNOWFLAKE_DATABASE}'
            """,
    ),
    TestConnection(  # connect using private key file
        test_name="private_key_file_connection",
        connection_yaml_str=f"""
                type: snowflake
                name: SNOWFLAKE_TEST
                connection:
                    account: '{SNOWFLAKE_ACCOUNT}'
                    user: '{SNOWFLAKE_USER}'
                    private_key_path: '{PRIVATE_KEY_FILE_PATH}'
                    private_key_passphrase: '{PRIVATE_KEY_PASSPHRASE}'
                    database: '{SNOWFLAKE_DATABASE}'
            """,
    ),
    TestConnection(  # connect using token
        test_name="token_connection",
        connection_yaml_str=f"""
                type: snowflake
                name: SNOWFLAKE_TEST
                connection:
                    account: '{SNOWFLAKE_ACCOUNT}'
                    user: '{SNOWFLAKE_USER}'
                    authenticator: 'oauth'
                    token: 'foo'
                    database: '{SNOWFLAKE_DATABASE}'
            """,
        valid_connection_params=False,
        expected_connection_error="Invalid OAuth access token",
    ),
    # TestConnection(  # connect using external browser. commented out by default because CI will hang.  Uncomment to test locally
    #     test_name="external_browser_connection",
    #     connection_yaml_str=f"""
    #             type: snowflake
    #             name: SNOWFLAKE_TEST
    #             connection:
    #                 account: '{SNOWFLAKE_ACCOUNT}'
    #                 user: '{SNOWFLAKE_USER}'
    #                 database: '{SNOWFLAKE_DATABASE}'
    #                 authenticator: 'externalbrowser'
    #         """,
    #     valid_connection_params=False,
    #     expected_connection_error="Failed to connect to DB"
    # ),
    TestConnection(  # connect using oauth credentials
        test_name="oauth_credentials_connection",
        connection_yaml_str=f"""
                type: snowflake
                name: SNOWFLAKE_TEST
                connection:
                    account: '{SNOWFLAKE_ACCOUNT}'
                    user: '{SNOWFLAKE_USER}'
                    authenticator: 'OAUTH_CLIENT_CREDENTIALS'
                    oauth_client_id: 'foo'
                    oauth_client_secret: 'foo'
                    oauth_token_request_url: 'foo'
                    oauth_scope: 'foo'
                    database: '{SNOWFLAKE_DATABASE}'
            """,
        valid_connection_params=False,
        expected_connection_error="Failed to resolve 'foo'",
    ),
]


# run tests.  parameterization means each test case will show up as an individual test
@pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
def test_snowflake_connections(test_connection: TestConnection):
    test_connection.test()
