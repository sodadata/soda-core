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


def test_importing_soda_core_contracts_eagerly_registers_in_fresh_interpreter():
    """Importing ``soda_core.contracts`` in a fresh Python interpreter
    eagerly wires the ``contract`` CheckCollection into the registry.

    Spawning a subprocess is load-bearing: in the same pytest process the
    contract module is already imported transitively by every other test
    module, so an in-process ``import soda_core.contracts; assert ...``
    smoke test would pass even if the load-bearing import at
    ``soda_core/contracts/__init__.py:9`` were removed. The subprocess
    forces a cold interpreter that exercises only the public package's
    ``__init__`` side-effect chain.

    Anyone who only imports the public facade
    (``from soda_core.contracts import verify_contract``) must be able to
    call ``CheckCollection.get('contract')`` without an extra side-effect
    import of the impl module.

    Note on regression coverage: the registration today has two load-bearing
    paths — (1) the explicit eager import at
    ``soda_core/contracts/__init__.py:9``, and (2) the transitive chain
    through ``soda_core.contracts.api`` → ``verify_api`` → check-type
    modules → ``contract_verification_impl``. This test exercises a fresh
    interpreter so it's not contaminated by other tests' imports. It would
    fail if BOTH paths regressed (e.g. someone factored the package
    differently). A single-line revert of (1) alone still passes here
    because of (2); the explicit eager import is documented in the
    ``__init__.py`` as deliberate single-source-of-truth for future
    maintainers who restructure the api package.
    """
    import subprocess
    import sys

    script = (
        "from soda_core.contracts import verify_contract\n"
        "from soda_core.check_collections.check_collection import CheckCollection\n"
        "descriptor = CheckCollection.get('contract')\n"
        "assert descriptor.kind == 'contract', descriptor.kind\n"
        "assert descriptor.impl_class.__name__ == 'ContractImpl', descriptor.impl_class.__name__\n"
        # Identity reads off the impl's ClassVars now — no duplicate descriptor fields.
        "assert descriptor.impl_class._WIRE_SOURCE == 'soda-contract'\n"
        "assert descriptor.impl_class._DISPLAY_NAME == 'contract'\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"Subprocess failed: stdout={result.stdout!r}, stderr={result.stderr!r}"
    # Match "OK" loosely so DeprecationWarning lines or other stdout noise (e.g.
    # from an import-time side effect) don't break the test. The subprocess
    # returncode + the presence of "OK" together prove the script ran the full
    # registry assertion chain.
    assert "OK" in result.stdout


def test_contract_facade_reraises_soda_core_exception_for_single_input():
    """The contract-typed facade preserves legacy single-input semantics: a
    single contract that fails with a ``SodaCoreException`` (e.g. malformed
    YAML → ``YamlParserException``) is re-raised verbatim instead of returned
    as an ERROR-status result. The impl is now symmetrically isolated (every
    ``Exception`` becomes an ERROR placeholder); the contract-typed facade
    re-raises for the single-input case to preserve callers that
    ``pytest.raises(YamlParserException)``.

    Multi-input callers and the universal facade never re-raise — they get the
    full ERROR-result list and can match errors to inputs by index.
    """
    from soda_core.common.exceptions import YamlParserException

    bad_yaml = "columns:\n  - name: id\n"  # missing required 'dataset' property
    import pytest

    with pytest.raises(YamlParserException):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(bad_yaml)],
            only_validate_without_execute=True,
        )


def test_universal_facade_does_not_reraise_isolated_errors():
    """The universal ``CheckCollectionVerificationSession.execute`` never
    re-raises — every per-spec exception is isolated into an ERROR-status
    placeholder so multi-collection callers (data-standards, future subtypes)
    get a uniform iteration shape with no per-slot ``None`` guards.

    Pins the asymmetry between universal facade (always isolated) and
    contract-typed facade (re-raises legacy single-input ``SodaCoreException``).
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    bad_yaml = "columns:\n  - name: id\n"  # missing required 'dataset' property

    result = CheckCollectionVerificationSession.execute(
        check_collection_yaml_sources=[ContractYamlSource.from_str(bad_yaml)],
        only_validate_without_execute=True,
    )

    assert len(result.check_collection_results) == 1
    error_result = result.check_collection_results[0]
    assert error_result.status is ContractVerificationStatus.ERROR


def test_contract_alias_points_to_check_collection_target():
    """``Contract`` is preserved as a module-level BC alias pointing at the
    universal ``CheckCollectionTarget`` boundary dataclass. External callers
    that ``from soda_core.contracts.contract_verification import Contract``
    keep working; soda-core internal code uses ``CheckCollectionTarget``.
    """
    from soda_core.check_collections.check_collection_verification import (
        CheckCollectionTarget as InternalTarget,
    )
    from soda_core.contracts.contract_verification import (
        CheckCollectionTarget as FacadeTarget,
    )
    from soda_core.contracts.contract_verification import Contract

    # The facade re-export and the internal symbol are the same class.
    assert FacadeTarget is InternalTarget
    # ``Contract`` is the BC alias for the same class.
    assert Contract is InternalTarget


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
