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

import pytest
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


@pytest.mark.parametrize(
    "kwarg_name",
    ["contract_yaml_sources", "check_collection_yaml_sources"],
    ids=["legacy_kwarg", "canonical_kwarg"],
)
def test_contract_session_returns_typed_result_on_either_kwarg(kwarg_name):
    """``ContractVerificationSession.execute`` accepts both the legacy
    ``contract_yaml_sources=`` and the canonical
    ``check_collection_yaml_sources=`` kwarg name; both return a
    ``ContractVerificationSessionResult`` carrying the typed BC alias.
    """
    result = ContractVerificationSession.execute(
        **{kwarg_name: [_yaml_source()]},
        only_validate_without_execute=True,
    )
    assert isinstance(result, ContractVerificationSessionResult)
    # The typed BC alias is what callers historically iterate.
    assert result.contract_verification_results == result.check_collection_results


@pytest.mark.parametrize(
    "kwarg_name, value_builder",
    [
        ("specs", lambda: [CheckCollectionSpec(kind="contract", yaml_source=_yaml_source())]),
        ("check_collection_yaml_sources", lambda: [_yaml_source()]),
    ],
    ids=["canonical_specs", "bc_kwarg"],
)
def test_universal_session_returns_base_result_on_either_input_shape(kwarg_name, value_builder):
    """``CheckCollectionVerificationSession.execute`` accepts both the canonical
    heterogeneous ``specs=`` input and the BC ``check_collection_yaml_sources=``
    kwarg; both return the base ``CheckCollectionSessionResult`` (NOT the
    contract-typed subtype — typed wrapping is the subtype facade's job).
    """
    result = CheckCollectionVerificationSession.execute(
        **{kwarg_name: value_builder()},
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


def test_contract_facade_does_not_reraise_for_multi_input():
    """The contract-typed facade re-raises ANY ``originating_exception`` ONLY
    when there's exactly one input (legacy single-contract semantics). When
    two or more contracts are passed, the bad slot is returned as an
    ERROR-status placeholder rather than re-raised so multi-input callers can
    match results to inputs by index.

    Pins the asymmetry: legacy single-input re-raise is preserved, but a
    future "fix" widening the re-raise scope to multi-input would silently
    break multi-collection callers — this test catches that.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    good_yaml = _CONTRACT_YAML
    bad_yaml = "columns:\n  - name: id\n"  # missing required 'dataset' property

    # No raise — multi-input path returns both ERROR + OK slots.
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(good_yaml),
            ContractYamlSource.from_str(bad_yaml),
        ],
        only_validate_without_execute=True,
    )
    assert isinstance(result, ContractVerificationSessionResult)
    assert len(result.check_collection_results) == 2
    # The bad slot is an ERROR placeholder carrying its originating exception.
    bad_result = result.check_collection_results[1]
    assert bad_result.status is ContractVerificationStatus.ERROR
    from soda_core.common.exceptions import YamlParserException

    assert isinstance(bad_result.originating_exception, YamlParserException)


def test_contract_facade_reraises_any_exception_for_single_input():
    """The contract-typed facade preserves legacy single-input semantics: a
    single contract that fails with ANY exception (``SodaCoreException``
    family for caller-input errors AND non-SodaCoreException for programming
    bugs / infrastructure failures) is re-raised verbatim instead of returned
    as an ERROR-status result. The impl is now symmetrically isolated (every
    ``Exception`` becomes an ERROR placeholder); the contract-typed facade
    re-raises for the single-input case so failures are noticed instead of
    silently masked into a result.

    Multi-input callers and the universal facade never re-raise — they get
    the full ERROR-result list and can match errors to inputs by index.
    """
    import pytest
    from soda_core.common.exceptions import YamlParserException

    bad_yaml = "columns:\n  - name: id\n"  # missing required 'dataset' property

    # Caller-input error (SodaCoreException family) propagates.
    with pytest.raises(YamlParserException):
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(bad_yaml)],
            only_validate_without_execute=True,
        )


def test_contract_facade_reraise_preserves_original_traceback():
    """The single-input re-raise in ``ContractVerificationSession.execute``
    must preserve ``exc.__traceback__`` so the original failure site is still
    visible in the traceback chain.

    ``raise exc`` (without ``from None``) keeps Python's traceback chain
    intact when the exception was previously raised and stored. If a future
    refactor switched to ``raise type(exc)(...)`` or ``raise exc from None``,
    the originating frame would be hidden and operators debugging from logs
    would lose the actual failure location. This test inspects the
    traceback frames and asserts the impl module (where the parse error
    originated) appears somewhere in the chain.
    """
    import pytest
    from soda_core.common.exceptions import YamlParserException

    bad_yaml = "columns:\n  - name: id\n"  # missing required 'dataset' property

    with pytest.raises(YamlParserException) as exc_info:
        ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(bad_yaml)],
            only_validate_without_execute=True,
        )

    # Walk the traceback frame chain — at least one frame should come from
    # below the facade (i.e. not from this test module and not from the
    # contract_verification facade itself). The originating failure inside
    # the impl module is what proves the traceback chain wasn't truncated.
    tb = exc_info.tb
    frame_files: list[str] = []
    while tb is not None:
        frame_files.append(tb.tb_frame.f_code.co_filename)
        tb = tb.tb_next
    # At least one frame from soda_core internal modules (not just this test
    # file + the facade re-raise frame). The exact module varies with the
    # exception path (yaml parser vs. impl bridge), so we just assert any
    # ``soda_core`` frame other than the facade exists.
    soda_core_frames = [f for f in frame_files if "soda_core" in f and "contract_verification.py" not in f]
    assert soda_core_frames, f"Re-raised traceback lost the originating frame chain. Frames seen: {frame_files}"


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


def test_check_file_location_property_aliases():
    """``Check.contract_file_line`` / ``contract_file_column`` are read-only
    ``@property`` aliases returning the new ``file_line`` / ``file_column``
    field values. Pins the BC contract for external callers reading those
    attributes off the public ``Check`` dataclass.
    """
    from soda_core.contracts.contract_verification import Check

    check = Check(
        column_name=None,
        type="missing",
        qualifier=None,
        name="missing on email",
        path="cols.email.checks.missing",
        identity="abc",
        definition="missing:",
        file_line=42,
        file_column=7,
        threshold=None,
        attributes=None,
        location=None,
    )

    # New field reads.
    assert check.file_line == 42
    assert check.file_column == 7
    # BC alias property reads — values follow the renamed fields.
    assert check.contract_file_line == 42
    assert check.contract_file_column == 7


def test_contract_verification_status_alias_points_to_check_collection_status():
    """``ContractVerificationStatus`` is preserved as a module-level BC alias
    pointing at the universal ``CheckCollectionStatus`` enum. Values are
    unchanged — only the class name moves. External callers that
    ``from soda_core.contracts.contract_verification import ContractVerificationStatus``
    keep working; soda-core internal code uses ``CheckCollectionStatus``.
    """
    from soda_core.check_collections.check_collection_verification import (
        CheckCollectionStatus as InternalStatus,
    )
    from soda_core.contracts.contract_verification import (
        CheckCollectionStatus as FacadeStatus,
    )
    from soda_core.contracts.contract_verification import ContractVerificationStatus

    assert FacadeStatus is InternalStatus
    assert ContractVerificationStatus is InternalStatus
    # Enum values unchanged.
    assert ContractVerificationStatus.PASSED is InternalStatus.PASSED
    assert ContractVerificationStatus.ERROR.value == "ERROR"


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
