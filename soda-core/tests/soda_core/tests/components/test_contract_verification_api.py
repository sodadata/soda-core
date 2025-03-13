from logging import CRITICAL, ERROR, INFO, WARN
from unittest.mock import Mock

import pytest
from soda_core.common.logs import Log, Logs
from soda_core.contracts.contract_verification import (
    ContractResult,
    ContractVerification,
    ContractVerificationResult,
    SodaException,
)


def test_contract_verification_file_api():
    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
        .with_variables({"env": "test"})
        .execute()
    )

    assert (
        "Contract file '../soda/mydb/myschema/table.yml' does not exist" in contract_verification_result.get_logs_str()
    )


def test_contract_verification_file_api_exception_on_error():
    with pytest.raises(SodaException) as e:
        ContractVerificationResult = (
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
        .with_contract_yaml_str(
            f"""
          dataset: CUSTOMERS
          columns:
            - name: id
        """
        )
        .with_variables({"env": "test"})
        .execute()
    )

    assert "No data source configured" in contract_verification_result.get_logs_str()


@pytest.mark.parametrize(
    "logs, contract_failed, has_errors, has_failures",
    [
        (Logs([Log(level=CRITICAL, message="critical")]), False, True, False),
        (Logs([Log(level=ERROR, message="error")]), False, True, False),
        (Logs([Log(level=ERROR, message="error"), Log(level=CRITICAL, message="critical")]), False, True, False),
        (Logs([Log(level=WARN, message="warn"), Log(level=INFO, message="info")]), False, False, False),
        (Logs([Log(level=WARN, message="warn"), Log(level=INFO, message="info")]), True, False, True),
        (Logs([Log(level=ERROR, message="error"), Log(level=CRITICAL, message="critical")]), True, True, True),
    ],
)
def test_contract_verification_log_levels(logs, contract_failed, has_errors, has_failures):
    mock_contract_result = Mock(spec=ContractResult)
    mock_contract_result.logs = logs
    mock_contract_result.failed.return_value = contract_failed

    result = ContractVerificationResult(logs=logs, contract_results=[mock_contract_result])

    assert result.has_errors() is has_errors
    assert result.has_failures() is has_failures
