import logging
from textwrap import dedent
from typing import List

from soda.contracts.contract import Contract
from soda.contracts.impl.logs import Log


def test_no_missing_with_must_be():
    contract_yaml_str = """
      dataset: TABLE_NAME
      columns:
        - name: id
          checks:
            - type: no_missing
              must_be: 5
    """

    contract_yaml_str = dedent(contract_yaml_str).strip()
    contract = Contract.from_yaml_str(contract_yaml_str=contract_yaml_str)
    errors: List[Log] = contract.logs.get_errors()
    assert errors

    assert "Check type 'no_missing' does not allow for threshold keys must_be_..." in str(errors[0])
