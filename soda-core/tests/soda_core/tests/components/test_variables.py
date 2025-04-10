from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def parse_contract(contract_yaml_str: str, variables: Optional[dict[str, str]] = None) -> ContractYaml:
    return ContractYaml.parse(
        contract_yaml_source=ContractYamlSource.from_str(dedent_and_strip(contract_yaml_str)), variables=variables
    )


def test_using_undeclared_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.DS_SUFFIX}
            dataset: d
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.data_source == "abc_${var.DS_SUFFIX}"

    assert (
        "Variable 'DS_SUFFIX' was used and provided, but not declared. "
        "Please add variable declaration to the contract" in logs.get_errors_str()
    )


def test_variable_declaration():
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.DS_SUFFIX}
            dataset: d

            variables:
              DS_SUFFIX:
                type: string
                required: false
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.data_source == "abc_xx"


def test_empty_variable_declaration():
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.DS_SUFFIX}
            dataset: d

            variables:
              DS_SUFFIX:
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.data_source == "abc_xx"


def test_variable_declaration_default():
    # Question: Shouldn't 'required' be derived from the presence of the 'default' key?

    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.DS_SUFFIX}
            dataset: d

            variables:
              DS_SUFFIX:
                type: string
                required: false
                default: xx
        """,
        variables={},
    )

    assert contract_yaml.data_source == "abc_xx"


def test_required_variable_not_provided(logs: Logs):
    parse_contract(
        contract_yaml_str="""
            data_source: abc
            dataset: d
            variables:
              DS_SUFFIX:
                type: string
                required: true
        """,
        variables={},
    )

    assert "Required variable 'DS_SUFFIX' not provided" in logs.get_errors_str()


def test_valid_timestamp_value(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.TS}
            dataset: d
            variables:
              TS:
                type: timestamp
                required: true
        """,
        variables={"TS": "2025-02-21T06:16:59Z"},
    )

    assert not logs.has_errors()
    assert contract_yaml.data_source == "abc_2025-02-21T06:16:59Z"


def test_invalid_timestamp_value(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${TS}
            dataset: d
            variables:
              TS:
                type: timestamp
                required: true
        """,
        variables={"TS": "buzzz"},
    )

    assert "Invalid timestamp value for variable 'TS': buzzz" in logs.get_errors_str()
    assert contract_yaml.data_source == "abc_${TS}"


def test_nested_variable_resolving(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.S1}
            dataset: abc_${var.S2}
            variables:
              S:
                required: true
              S1:
                default: 1_${var.S}
              S2:
                default: 2_${var.S}
        """,
        variables={"S": "X"},
    )

    assert contract_yaml.data_source == "abc_1_X"
    assert contract_yaml.dataset == "abc_2_X"
    assert "" in logs.get_errors_str()


def test_now_variable_default_availability(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.NOW}
            dataset: d
        """,
        variables={},
    )

    assert contract_yaml.data_source != "abc_${var.NOW}"
    assert "" == logs.get_errors_str()


def test_provided_now_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.NOW}
            dataset: d
        """,
        variables={"NOW": "2025-02-21T06:16:59Z"},
    )

    assert contract_yaml.data_source == "abc_2025-02-21T06:16:59Z"
    assert "" == logs.get_errors_str()


def test_provided_invalid_now_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: abc_${var.NOW}
            dataset: d
        """,
        variables={"NOW": "buzz"},
    )

    assert contract_yaml.data_source != "abc_${var.NOW}"
    assert "Provided 'NOW' variable value is not a correct timestamp format: buzz" == logs.get_errors_str()


def test_variables_example_in_docs(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            data_source: ${var.SEGMENT_FILTER}
            dataset: d
            variables:
              COUNTRY:
                default: USA
              CATEGORY:
                default: L
              CREATED_AFTER_TS:
              SEGMENT_FILTER:
                default: |
                    country ='${var.COUNTRY}'
                    AND category = '${var.CATEGORY}'
                    AND created_at <= TIMESTAMP '${var.NOW}'
                    AND created_at > TIMESTAMP '${var.CREATED_AFTER_TS}'
    """,
        variables={"CATEGORY": "M", "CREATED_AFTER_TS": "2025-02-21T06:16:59Z", "NOW": "2025-04-04T06:04:40+00:00"},
    )

    assert dedent_and_strip(contract_yaml.data_source) == dedent_and_strip(
        """
        country ='USA'
        AND category = 'M'
        AND created_at <= TIMESTAMP '2025-04-04T06:04:40+00:00'
        AND created_at > TIMESTAMP '2025-02-21T06:16:59Z'
    """
    )
    assert "" == logs.get_errors_str()
