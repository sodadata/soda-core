from textwrap import dedent

from contracts.helpers.contract_fixtures import *  # NOQA
from helpers.fixtures import *  # NOQA

from soda.contracts.contract_verification import ContractVerification
from soda.contracts.impl.logs import Log


def get_parse_errors_str(contract_yaml_str: str) -> str:
    contract_yaml_str = dedent(contract_yaml_str).strip()
    contract_verification_builder = ContractVerification.builder().with_contract_yaml_str(
        contract_yaml_str=contract_yaml_str
    )
    contract_verification = contract_verification_builder.build()
    errors: list[Log] = contract_verification.logs.get_errors()
    return "\n".join([str(e) for e in errors])
