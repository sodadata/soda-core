from textwrap import dedent

from contracts.helpers.contract_fixtures import *  # NOQA
from helpers.fixtures import *  # NOQA

from soda.contracts.contract import Contract
from soda.contracts.impl.logs import Log


def get_parse_errors_str(contract_yaml_str: str) -> str:
    contract_yaml_str = dedent(contract_yaml_str).strip()
    contract = Contract.from_yaml_str(contract_yaml_str=contract_yaml_str)
    contract._Contract__parse()
    errors: list[Log] = contract.logs.get_errors()
    return "\n".join([str(e) for e in errors])
