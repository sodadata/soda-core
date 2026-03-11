"""
Helpers for CheckImpl tests.

Uses the validate-without-execute path to construct ContractImpl objects
without needing a real data source connection.
"""

from __future__ import annotations

from typing import Optional

from helpers.test_functions import dedent_and_strip
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def validate_contract(yaml_str: str, variables: Optional[dict[str, str]] = None):
    """
    Run contract validation (without execution) and return the session result.
    This builds ContractImpl objects but does not run any SQL queries.
    """
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(yaml_str))],
        only_validate_without_execute=True,
        variables=variables,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )
    return result
