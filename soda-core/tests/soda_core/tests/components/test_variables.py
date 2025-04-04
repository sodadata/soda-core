from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.tests.helpers.test_functions import dedent_and_strip


def parse_contract(contract_yaml_str: str, variables: Optional[dict[str, str]] = None) -> ContractYaml:
    return ContractYaml.parse(
        contract_yaml_source=YamlSource.from_str(dedent_and_strip(contract_yaml_str)), variables=variables
    )


def test_using_undeclared_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d
            checks:
              - schema:
                  name: abc_${var.DS_SUFFIX}
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.checks[0].name == "abc_${var.DS_SUFFIX}"

    assert (
        "Variable 'DS_SUFFIX' was used and provided, but not declared. "
        "Please add variable declaration to the contract" in logs.get_errors_str()
    )


def test_variable_declaration():
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            variables:
              DS_SUFFIX:
                type: string
                required: false

            checks:
              - schema:
                  name: abc_${var.DS_SUFFIX}
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.checks[0].name == "abc_xx"


def test_empty_variable_declaration():
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            variables:
              DS_SUFFIX:

            checks:
              - schema:
                  name: abc_${var.DS_SUFFIX}
        """,
        variables={"DS_SUFFIX": "xx"},
    )

    assert contract_yaml.checks[0].name == "abc_xx"


def test_variable_declaration_default():
    # Question: Shouldn't 'required' be derived from the presence of the 'default' key?

    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            variables:
              DS_SUFFIX:
                type: string
                required: false
                default: xx

            checks:
              - schema:
                  name: abc_${var.DS_SUFFIX}
        """,
        variables={},
    )

    assert contract_yaml.checks[0].name == "abc_xx"


def test_required_variable_not_provided(logs: Logs):
    parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d
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
            dataset: a/b/c/d

            variables:
              TS:
                type: timestamp
                required: true

            checks:
              - schema:
                  name: abc_${var.TS}
        """,
        variables={"TS": "2025-02-21T06:16:59Z"},
    )

    assert not logs.has_errors()
    assert contract_yaml.checks[0].name == "abc_2025-02-21T06:16:59Z"


def test_invalid_timestamp_value(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            variables:
              TS:
                type: timestamp
                required: true

            checks:
              - schema:
                  name: abc_${TS}
        """,
        variables={"TS": "buzzz"},
    )

    assert "Invalid timestamp value for variable 'TS': buzzz" in logs.get_errors_str()
    assert contract_yaml.checks[0].name == "abc_${TS}"


def test_nested_variable_resolving(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            variables:
              S:
                required: true
              S1:
                default: 1_${var.S}
              S2:
                default: 2_${var.S}

            checks:
              - schema:
                  name: abc_${var.S1}
              - schema:
                  name: abc_${var.S2}
                  qualifier: 2
        """,
        variables={"S": "X"},
    )

    assert contract_yaml.checks[0].name == "abc_1_X"
    assert contract_yaml.checks[1].name == "abc_2_X"
    assert "" in logs.get_errors_str()


def test_now_variable_default_availability(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            checks:
              - schema:
                  name: abc_${var.NOW}
        """,
        variables={},
    )

    assert contract_yaml.checks[0].name != "abc_${var.NOW}"
    assert "" == logs.get_errors_str()


def test_provided_now_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            checks:
              - schema:
                  name: abc_${var.NOW}
        """,
        variables={"NOW": "2025-02-21T06:16:59Z"},
    )

    assert contract_yaml.checks[0].name == "abc_2025-02-21T06:16:59Z"
    assert "" == logs.get_errors_str()


def test_provided_invalid_now_variable(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

            checks:
              - schema:
                  name: abc_${var.NOW}
        """,
        variables={"NOW": "buzz"},
    )

    assert contract_yaml.checks[0].name != "abc_${var.NOW}"
    assert "Provided 'NOW' variable value is not a correct timestamp format: buzz" == logs.get_errors_str()


def test_variables_example_in_docs(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: a/b/c/d

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

            checks:
              - schema:
                  name: ${var.SEGMENT_FILTER}
    """,
        variables={"CATEGORY": "M", "CREATED_AFTER_TS": "2025-02-21T06:16:59Z", "NOW": "2025-04-04T06:04:40+00:00"},
    )

    assert dedent_and_strip(contract_yaml.checks[0].name) == dedent_and_strip(
        """
        country ='USA'
        AND category = 'M'
        AND created_at <= TIMESTAMP '2025-04-04T06:04:40+00:00'
        AND created_at > TIMESTAMP '2025-02-21T06:16:59Z'
    """
    )
    assert "" == logs.get_errors_str()
