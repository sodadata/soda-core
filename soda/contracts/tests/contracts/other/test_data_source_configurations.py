import os
from textwrap import dedent

from soda.contracts.contract_verification import ContractVerification, SodaException


def test_data_source_error_file_not_found():
    contract_verification = (
        ContractVerification.builder().with_data_source_yaml_file("./non_existing_file.scn.yml").build()
    )
    contract_verification_str = str(contract_verification)
    assert "File './non_existing_file.scn.yml' does not exist" in contract_verification_str


def test_data_source_file_variable_resolving(environ):
    environ["POSTGRES_DATABASE"] = "sodasql"
    environ["POSTGRES_USERNAME"] = "sodasql"

    data_source_file_path = os.path.join(os.path.dirname(__file__), "test_data_source_configurations.yml")

    contract_verification = ContractVerification.builder().with_data_source_yaml_file(data_source_file_path).build()

    resolved_connection_properties = contract_verification.data_source.data_source_file.dict["connection"]
    assert "sodasql" == resolved_connection_properties["database"]
    assert "sodasql" == resolved_connection_properties["username"]


def test_invalid_database():
    data_source_yaml_str = dedent(
        """
            name: postgres_ds
            type: postgres
            connection:
              host: localhost
              database: invalid_db
              username: sodasql
              port: ${POSTGRES_PORT}
        """
    )

    contract_verification = ContractVerification.builder().with_data_source_yaml_str(data_source_yaml_str).execute()

    contract_verification_str = str(contract_verification)
    assert "Could not connect to 'postgres_ds'" in contract_verification_str
    assert 'database "invalid_db" does not exist' in contract_verification_str


def test_invalid_username():
    data_source_yaml_str = dedent(
        """
            name: postgres_ds
            type: postgres
            connection:
              host: localhost
              database: sodasql
              username: invalid_usr
              port: ${POSTGRES_PORT}
        """
    )

    try:
        (ContractVerification.builder().with_data_source_yaml_str(data_source_yaml_str).execute().assert_ok())
        raise AssertionError("Expected SodaException from the .assert_no_problems()")
    except SodaException as e:
        exception_message = str(e)
        assert "Could not connect to 'postgres_ds'" in exception_message
        assert 'role "invalid_usr" does not exist' in exception_message
