"""
Helpers for YAML parsing tests.

Wraps ContractYamlSource → ContractYaml.parse() to reduce boilerplate.
"""

from __future__ import annotations

from typing import Optional

from helpers.test_functions import dedent_and_strip
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, ContractYaml


def parse_contract(yaml_str: str, variables: Optional[dict[str, str]] = None) -> ContractYaml:
    """Parse a contract YAML string into a ContractYaml domain model."""
    contract_yaml_source = ContractYamlSource.from_str(yaml_str=dedent_and_strip(yaml_str))
    return ContractYaml.parse(
        contract_yaml_source=contract_yaml_source,
        provided_variable_values=variables or {},
    )


def parse_check_from_contract(yaml_str: str, check_index: int = 0) -> CheckYaml:
    """Parse a contract YAML and return a dataset-level check by index."""
    contract_yaml = parse_contract(yaml_str)
    assert contract_yaml.checks is not None, "Contract has no dataset-level checks"
    assert (
        len(contract_yaml.checks) > check_index
    ), f"Contract has only {len(contract_yaml.checks)} checks, requested index {check_index}"
    return contract_yaml.checks[check_index]


def parse_column_check(
    yaml_str: str,
    col_index: int = 0,
    check_index: int = 0,
) -> CheckYaml:
    """Parse a contract YAML and return a column-level check by indices."""
    contract_yaml = parse_contract(yaml_str)
    assert contract_yaml.columns is not None, "Contract has no columns"
    assert (
        len(contract_yaml.columns) > col_index
    ), f"Contract has only {len(contract_yaml.columns)} columns, requested index {col_index}"
    column_yaml: ColumnYaml = contract_yaml.columns[col_index]
    assert column_yaml.check_yamls is not None, f"Column '{column_yaml.name}' has no checks"
    assert (
        len(column_yaml.check_yamls) > check_index
    ), f"Column '{column_yaml.name}' has only {len(column_yaml.check_yamls)} checks, requested index {check_index}"
    return column_yaml.check_yamls[check_index]


def parse_column(yaml_str: str, col_index: int = 0) -> ColumnYaml:
    """Parse a contract YAML and return a column by index."""
    contract_yaml = parse_contract(yaml_str)
    assert contract_yaml.columns is not None, "Contract has no columns"
    assert (
        len(contract_yaml.columns) > col_index
    ), f"Contract has only {len(contract_yaml.columns)} columns, requested index {col_index}"
    return contract_yaml.columns[col_index]
