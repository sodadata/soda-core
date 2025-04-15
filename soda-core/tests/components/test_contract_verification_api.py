import pytest

from soda_core.common.exceptions import InvalidDatasetQualifiedNameException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
    SodaException,
)


def test_contract_verification_file_api():
    with pytest.raises(InvalidDatasetQualifiedNameException):
        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_file_path("../soda/mydb/myschema/table.yml")],
            variables={"env": "test"},
        )

        assert (
            "Contract file '../soda/mydb/myschema/table.yml' does not exist"
            in contract_verification_session_result.get_errors_str()
        )


def test_contract_verification_file_api_exception_on_error():
    with pytest.raises(InvalidDatasetQualifiedNameException):
        with pytest.raises(SodaException) as e:
            ContractVerificationSession.execute(
                contract_yaml_sources=[ContractYamlSource.from_file_path("../soda/mydb/myschema/table.yml")],
                variables={"env": "test"},
            ).assert_ok()

            exception_string = str(e.value)
            assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in exception_string


def test_contract_provided_and_configured():
    """
    If there is no default data source configured and there is none provided in the contract, an error has to be logged
    """
    contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(
                f"""
              dataset: abc/CUSTOMERS
              columns:
                - name: id
            """
            )
        ],
    )

    assert "Data source 'abc' not found" in contract_verification_session_result.get_errors_str()


# TODO @Niels: To be evaluated if still needed refactored after rework
# @pytest.mark.parametrize(
#     "logs, contract_failed, has_errors, has_failures",
#     [
#         (Logs([Log(level=CRITICAL, message="critical")]), False, True, False),
#         (Logs([Log(level=ERROR, message="error")]), False, True, False),
#         (Logs([Log(level=ERROR, message="error"), Log(level=CRITICAL, message="critical")]), False, True, False),
#         (Logs([Log(level=WARN, message="warn"), Log(level=INFO, message="info")]), False, False, False),
#         (Logs([Log(level=WARN, message="warn"), Log(level=INFO, message="info")]), True, False, True),
#         (Logs([Log(level=ERROR, message="error"), Log(level=CRITICAL, message="critical")]), True, True, True),
#     ],
# )
# def test_contract_verification_log_levels(logs, contract_failed, has_errors, has_failures):
#     mock_contract_result = Mock(spec=ContractResult)
#     mock_contract_result.logs = logs
#     mock_contract_result.failed.return_value = contract_failed
#
#     result = ContractVerificationResult(logs=logs, contract_results=[mock_contract_result])
#
#     assert result.has_errors() is has_errors
#     assert result.has_failures() is has_failures
