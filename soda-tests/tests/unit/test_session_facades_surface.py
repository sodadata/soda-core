"""Smoke tests for the public session facade surface.

Pins the four supported invocation shapes after Task 3 of the soda-core
session-impl reshape:

* ``ContractVerificationSession.execute(contract_yaml_sources=...)``
  — legacy contract-typed entry; returns ``ContractVerificationSessionResult``.
* ``ContractVerificationSession.execute(check_collection_yaml_sources=...)``
  — canonical kwarg on the contract-typed facade; same typed result.
* ``CheckCollectionVerificationSession.execute(specs=[CheckCollectionSpec(...)])``
  — canonical heterogeneous input; returns the universal base result.
* ``CheckCollectionVerificationSession.execute(check_collection_yaml_sources=...)``
  — BC kwarg on the universal facade; same universal base result.

All four use ``only_validate_without_execute=True`` to avoid hitting a DB.
"""

from __future__ import annotations

from soda_core.check_collections.check_collection_spec import CheckCollectionSpec
from soda_core.check_collections.check_collection_verification import (
    CheckCollectionSessionResult,
    CheckCollectionVerificationSession,
)
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)

_CONTRACT_YAML = """\
dataset: datasource/db/dataset/orders
columns:
  - name: id
    data_type: integer
    checks:
      - missing:
"""


def _yaml_source() -> ContractYamlSource:
    return ContractYamlSource.from_str(_CONTRACT_YAML)


def test_contract_session_with_legacy_kwarg_returns_typed_result():
    """ContractVerificationSession.execute(contract_yaml_sources=...) — legacy entry."""
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[_yaml_source()],
        only_validate_without_execute=True,
    )
    assert isinstance(result, ContractVerificationSessionResult)
    # The typed BC alias is what callers historically iterate.
    assert result.contract_verification_results == result.check_collection_results


def test_contract_session_with_canonical_kwarg_returns_typed_result():
    """ContractVerificationSession.execute(check_collection_yaml_sources=...) — canonical kwarg name."""
    result = ContractVerificationSession.execute(
        check_collection_yaml_sources=[_yaml_source()],
        only_validate_without_execute=True,
    )
    assert isinstance(result, ContractVerificationSessionResult)


def test_universal_session_with_specs_returns_base_result():
    """CheckCollectionVerificationSession.execute(specs=...) — canonical heterogeneous entry."""
    spec = CheckCollectionSpec(kind="contract", yaml_source=_yaml_source())
    result = CheckCollectionVerificationSession.execute(
        specs=[spec],
        only_validate_without_execute=True,
    )
    assert isinstance(result, CheckCollectionSessionResult)
    # Typed wrapping is the subtype facade's job — the universal facade must
    # not pre-wrap into the contract-typed subtype.
    assert not isinstance(result, ContractVerificationSessionResult)
    assert len(result.check_collection_results) == 1


def test_universal_session_with_bc_kwarg_returns_base_result():
    """CheckCollectionVerificationSession.execute(check_collection_yaml_sources=...) — BC kwarg path."""
    result = CheckCollectionVerificationSession.execute(
        check_collection_yaml_sources=[_yaml_source()],
        only_validate_without_execute=True,
    )
    assert isinstance(result, CheckCollectionSessionResult)
    assert not isinstance(result, ContractVerificationSessionResult)
    assert len(result.check_collection_results) == 1
