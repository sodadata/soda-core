import pytest

from soda_core.contracts.contract_verification import ContractVerificationResult, ContractVerification, SodaException


def test_contract_verification_file_api():
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in str(contract_verification_result)


def test_contract_verification_file_api_exception_on_error():
    with pytest.raises(SodaException) as e:
        contract_verification_result: ContractVerificationResult = (
            ContractVerification.builder()
            .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
            .with_variables({"env": "test"})
            .execute()
            .assert_ok()
        )

    exception_string = str(e.value)
    assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in exception_string


def test_contract_provided_and_configured():
    """
    If there is no default data source configured and there is none provided in the contract, an error has to be logged
    """
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_str(f"""
          dataset: CUSTOMERS
          columns:
            - name: id
        """)
        .with_variables({"env": "test"})
        .execute()
    )

    assert "No data source configured" in str(contract_verification_result)
