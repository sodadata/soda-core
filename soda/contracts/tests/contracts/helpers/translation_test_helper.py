from __future__ import annotations

from soda.contracts.contract import Contract
from soda.contracts.impl.contract_parser import ContractParser
from soda.contracts.impl.logs import Logs


def translate(contract_yaml_str: str) -> str:
    sodacl_yaml_str, logs = translate_with_logs(contract_yaml_str)
    logs.assert_no_errors()
    return sodacl_yaml_str


def translate_with_logs(contract_yaml_str) -> (str, Logs):
    logs = Logs()
    contract_translator = ContractParser()
    variables = {}
    sodacl_yaml_str: str | None = Contract._translate_contract_to_sodacl(
        contract_translator=contract_translator,
        contract_yaml_str=contract_yaml_str,
        logs=logs,
        variables=variables
    )
    sodacl_yaml_str = sodacl_yaml_str.strip() if isinstance(sodacl_yaml_str, str) else None
    return sodacl_yaml_str, logs
