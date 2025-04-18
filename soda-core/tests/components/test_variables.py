from typing import Optional

from helpers.test_functions import dedent_and_strip
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml


def parse_contract(contract_yaml_str: str, variables: Optional[dict[str, str]] = None) -> ContractYaml:
    return ContractYaml.parse(
        contract_yaml_source=ContractYamlSource.from_str(dedent_and_strip(contract_yaml_str)),
        provided_variable_values=variables,
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

    assert "Variable 'DS_SUFFIX' was used and not declared" in logs.get_errors_str()


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

    assert "Provided 'NOW' variable value is not a correct ISO 8601 timestamp format: buzz" == logs.get_errors_str()
    assert contract_yaml.checks[0].name == "abc_buzz"


def test_variables_example1_in_docs(logs: Logs):
    contract_yaml: ContractYaml = parse_contract(
        contract_yaml_str="""
            dataset: postgres_adventureworks/adventureworks/${var.DATASET_SCHEMA}/${var.DATASET_PREFIX}_employee

            variables:
              DATASET_SCHEMA:
                default: advw
              DATASET_PREFIX:
                default: dim

            columns:
              - name: id
    """
    )

    assert dedent_and_strip(contract_yaml.dataset) == ("postgres_adventureworks/adventureworks/advw/dim_employee")
    assert "" == logs.get_errors_str()
