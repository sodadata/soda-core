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


def test_importing_soda_core_contracts_registers_contract_family():
    """Importing the public ``soda_core.contracts`` package eagerly wires the
    ``contract`` family into the registry. Anyone who only imports the
    public facade (i.e. ``from soda_core.contracts import verify_contract``)
    can call ``get_family("contract")`` without an extra side-effect import
    of the impl module.

    This pins the move of the family-registering side-effect from a lazy
    import inside ``CheckCollectionVerificationSession.execute`` to the
    public package's ``__init__`` — registration now happens once on first
    facade touch, not on every execute() call.
    """
    import soda_core.contracts  # noqa: F401  — load-bearing import for this test
    from soda_core.check_collections.check_collection_family import get_family

    family = get_family("contract")
    assert family.kind == "contract"
    # Identity is read off the impl's ClassVars now — no duplicate family fields.
    assert family.impl_class._WIRE_SOURCE == "soda-contract"
    assert family.impl_class._DISPLAY_NAME == "contract"


def test_contract_wire_source_is_plumbed_from_classvar_to_contract():
    """The ``Contract`` boundary object carries ``wire_source`` populated from
    ``type(check_collection_impl)._WIRE_SOURCE`` at construction time, so the
    Cloud upload site can read it instead of hardcoding ``"soda-contract"``.

    The check is structural — we don't run a real upload; we assert the
    plumbing path:
    1. ContractImpl declares ``_WIRE_SOURCE = "soda-contract"`` (ClassVar).
    2. A locally-verified contract's ``ContractVerificationResult.contract``
       carries the same value on ``wire_source``.

    Pins the fix for the hardcoded ``"source": "soda-contract"`` in
    ``soda_cloud._build_check_result_cloud_dict`` — when a future subtype
    declares ``_WIRE_SOURCE = "data-standard"``, the upload will say
    ``"data-standard"`` automatically.
    """
    from soda_core.contracts.contract_verification import ContractVerificationSession
    from soda_core.contracts.impl.contract_verification_impl import ContractImpl

    # 1. ClassVar is the source of truth.
    assert ContractImpl._WIRE_SOURCE == "soda-contract"

    # 2. The plumbed value reaches Contract.wire_source on the result.
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[_yaml_source()],
        only_validate_without_execute=True,
    )
    assert len(result.check_collection_results) == 1
    contract = result.check_collection_results[0].contract
    assert contract.wire_source == "soda-contract"
