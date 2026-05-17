"""Pins the universal session impl's dispatch behavior over the
CheckCollection registry: mixed-kind sessions aggregate per-spec results in
input order, unknown kinds raise a clear lookup error, and the agent path
raises a clear ``NotImplementedError`` for descriptors registered with
``on_agent_verifier=None``.

Replaces the session-side Generic-derivation tests that were dropped when
``CheckCollectionVerificationSessionImpl`` stopped subclassing
``Generic[YamlT, ImplT, ResultT]`` — dispatch is now per-spec via the
registry rather than via a session subclass.
"""

from __future__ import annotations

from typing import Iterator

import pytest
from soda_core.check_collections.check_collection import CheckCollection
from soda_core.check_collections.check_collection_spec import CheckCollectionSpec
from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
    CheckCollectionSessionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
    CheckCollectionVerificationSessionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml
from soda_core.common.yaml import CheckCollectionYamlSource

# Two sentinel descriptors register at test time. We snapshot/restore the
# class-level registry so they don't leak across tests.


class _SentinelAYaml(CheckCollectionYaml):
    _KIND = "sentinel_a"


class _SentinelAResult(CheckCollectionResult):
    pass


class _SentinelAImpl(CheckCollectionImpl[_SentinelAYaml, _SentinelAResult]):
    _DISPLAY_NAME = "sentinel-a"
    _WIRE_SOURCE = "soda-sentinel-a"


class _SentinelBYaml(CheckCollectionYaml):
    _KIND = "sentinel_b"


class _SentinelBResult(CheckCollectionResult):
    pass


class _SentinelBImpl(CheckCollectionImpl[_SentinelBYaml, _SentinelBResult]):
    _DISPLAY_NAME = "sentinel-b"
    _WIRE_SOURCE = "soda-sentinel-b"


_MINIMAL_YAML = """\
dataset: datasource/db/schema/orders
columns:
  - name: id
    checks:
      - missing:
"""


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    """Snapshot the registry around each test so sentinel descriptors don't leak."""
    saved = dict(CheckCollection._REGISTRY)
    try:
        yield
    finally:
        CheckCollection._REGISTRY.clear()
        CheckCollection._REGISTRY.update(saved)


def _yaml_source() -> CheckCollectionYamlSource:
    return CheckCollectionYamlSource.from_str(_MINIMAL_YAML)


def _register_sentinel_a(*, on_agent_verifier=None) -> None:
    CheckCollection.register(
        kind="sentinel_a",
        yaml_class=_SentinelAYaml,
        impl_class=_SentinelAImpl,
        on_agent_verifier=on_agent_verifier,
    )


def _register_sentinel_b(*, on_agent_verifier=None) -> None:
    CheckCollection.register(
        kind="sentinel_b",
        yaml_class=_SentinelBYaml,
        impl_class=_SentinelBImpl,
        on_agent_verifier=on_agent_verifier,
    )


def test_mixed_kind_session_aggregates_per_spec_results_in_order():
    """A heterogeneous ``specs=`` list dispatches each spec to its CheckCollection
    descriptor and aggregates the per-spec results in input order on the session result."""
    _register_sentinel_a()
    _register_sentinel_b()

    specs = [
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_b", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
    ]

    result = CheckCollectionVerificationSessionImpl.execute(
        specs=specs,
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert isinstance(result, CheckCollectionSessionResult)
    assert len(result.check_collection_results) == 3
    # Input order is preserved: A, B, A → per-spec result types should mirror.
    per_spec_types = [type(r) for r in result.check_collection_results]
    assert per_spec_types == [_SentinelAResult, _SentinelBResult, _SentinelAResult]


def test_unknown_kind_isolates_as_error_placeholder_with_known_kinds_list():
    """A spec referencing an unregistered ``kind`` is isolated per-spec — the
    session does not abort, the result list stays positional, and the placeholder
    carries a ``KeyError`` whose message names the kind and the known list.

    Symmetric with the rest of per-spec isolation: structurally invalid input
    (bad kind) produces an ERROR placeholder rather than crashing the session.
    Backend ingestion can still match the placeholder to its input spec by
    index even when the registration was broken.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    _register_sentinel_a()

    spec = CheckCollectionSpec(kind="unregistered", yaml_source=_yaml_source())

    result = CheckCollectionVerificationSessionImpl.execute(
        specs=[spec],
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )
    assert len(result.check_collection_results) == 1
    error_result = result.check_collection_results[0]
    assert error_result.status is ContractVerificationStatus.ERROR
    exc = error_result._internal_exception
    assert isinstance(exc, KeyError)
    message = str(exc)
    assert "unregistered" in message
    assert "Known kinds:" in message


def test_agent_path_with_on_agent_verifier_none_isolates_as_error_placeholder():
    """A descriptor registered with ``on_agent_verifier=None`` produces an
    ERROR-status placeholder carrying a ``NotImplementedError`` rather than
    aborting the session.

    Symmetric with the local-execute path: every ``Exception`` subtype is
    isolated into a per-spec placeholder so multi-spec sessions stay robust
    even when one descriptor doesn't support agent execution.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    _register_sentinel_a(on_agent_verifier=None)

    spec = CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source())

    result = CheckCollectionVerificationSessionImpl.execute(
        specs=[spec],
        soda_cloud_use_agent=True,
        soda_cloud_publish_results=False,
    )
    assert len(result.check_collection_results) == 1
    error_result = result.check_collection_results[0]
    assert error_result.status is ContractVerificationStatus.ERROR
    exc = error_result._internal_exception
    assert isinstance(exc, NotImplementedError)
    message = str(exc)
    assert "sentinel-a" in message
    assert "agent execution" in message


def test_spec_collection_name_is_forwarded_to_impl():
    """``spec.collection_name`` reaches the constructed impl as
    ``check_collection_impl.collection_name``.

    Captures the impl constructed for the spec via a tiny subclass override
    so the test can introspect the post-construction attribute without
    needing a real verification run.
    """
    captured: list[_SentinelAImpl] = []

    class _CapturingSentinelAImpl(_SentinelAImpl):
        _WIRE_SOURCE = "soda-sentinel-a"

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured.append(self)

    CheckCollection.register(
        kind="sentinel_a",
        yaml_class=_SentinelAYaml,
        impl_class=_CapturingSentinelAImpl,
        on_agent_verifier=None,
    )

    spec = CheckCollectionSpec(
        kind="sentinel_a",
        yaml_source=_yaml_source(),
        collection_name="foo",
    )
    CheckCollectionVerificationSessionImpl.execute(
        specs=[spec],
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert len(captured) == 1
    assert captured[0].collection_name == "foo"


def test_per_spec_error_isolation_in_local_execute():
    """An unexpected runtime error on one spec must not abort the session — later
    specs still run and the result list stays positional with the input.

    Three specs: good, broken, good. The middle spec's impl ``verify()`` raises
    a plain ``RuntimeError`` (simulating an unexpected runtime bug). The result
    list has length 3 with an ERROR-status ``CheckCollectionResult`` placeholder
    in the middle slot and real results at the edges. Consumers (e.g. backend
    ingestion) may match results to inputs by index, so the positional contract
    matters and the placeholder shape must be a real ``CheckCollectionResult``
    so consumers don't need ``None`` guards.

    Note: ``SodaCoreException`` subclasses (YamlParserException etc.) are also
    isolated at the impl level — the contract-typed facade
    (``ContractVerificationSession.execute``) re-raises them for legacy
    single-input callers; the universal facade never does.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    # Per-test sentinel impl that raises during verify(). We register a fresh
    # descriptor with this impl so the failure is genuinely per-spec.
    class _BrokenSentinelImpl(_SentinelAImpl):
        _WIRE_SOURCE = "soda-sentinel-broken"

        def verify(self):
            raise RuntimeError("simulated unexpected runtime failure")

    class _BrokenSentinelYaml(_SentinelAYaml):
        _KIND = "sentinel_broken"

    CheckCollection.register(
        kind="sentinel_broken",
        yaml_class=_BrokenSentinelYaml,
        impl_class=_BrokenSentinelImpl,
        on_agent_verifier=None,
    )
    _register_sentinel_a()

    specs = [
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_broken", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
    ]

    result = CheckCollectionVerificationSessionImpl.execute(
        specs=specs,
        only_validate_without_execute=False,  # need verify() to be called for the broken spec
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert len(result.check_collection_results) == 3
    # Positional: edges are real results, middle is the ERROR placeholder.
    assert isinstance(result.check_collection_results[0], _SentinelAResult)
    error_result = result.check_collection_results[1]
    assert isinstance(error_result, CheckCollectionResult)
    assert error_result.status is ContractVerificationStatus.ERROR
    assert error_result.has_errors is True
    # The originating exception is stored privately on the placeholder so the
    # contract-typed facade can re-raise it for legacy single-input callers.
    assert isinstance(error_result._internal_exception, RuntimeError)
    assert isinstance(result.check_collection_results[2], _SentinelAResult)


def test_soda_core_exception_is_isolated_at_impl_level():
    """``SodaCoreException`` (and subclasses like ``YamlParserException``) raised
    inside per-spec processing is also isolated at the impl level — the
    universal facade never re-raises. The contract-typed facade
    (``ContractVerificationSession.execute``) re-raises for legacy single-input
    callers; that legacy preservation lives at the facade, not at the impl.

    Pins the symmetric impl-level contract: every ``Exception`` subtype gets
    isolated into an ERROR-status placeholder.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )
    from soda_core.common.exceptions import SodaCoreException

    class _SodaCoreFailureImpl(_SentinelAImpl):
        _WIRE_SOURCE = "soda-sentinel-soda-core-fail"

        def verify(self):
            raise SodaCoreException("simulated caller-input error")

    class _SodaCoreFailureYaml(_SentinelAYaml):
        _KIND = "sentinel_soda_core_fail"

    CheckCollection.register(
        kind="sentinel_soda_core_fail",
        yaml_class=_SodaCoreFailureYaml,
        impl_class=_SodaCoreFailureImpl,
        on_agent_verifier=None,
    )

    spec = CheckCollectionSpec(kind="sentinel_soda_core_fail", yaml_source=_yaml_source())

    # Universal facade / direct impl call must NOT raise — every Exception
    # subtype is isolated at the impl level.
    result = CheckCollectionVerificationSessionImpl.execute(
        specs=[spec],
        only_validate_without_execute=False,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert len(result.check_collection_results) == 1
    error_result = result.check_collection_results[0]
    assert isinstance(error_result, CheckCollectionResult)
    assert error_result.status is ContractVerificationStatus.ERROR
    assert isinstance(error_result._internal_exception, SodaCoreException)


def test_per_spec_error_isolation_in_agent_execute():
    """The agent-execute path mirrors the local-execute isolation contract:
    a failing on_agent_verifier raises an ``Exception``, the spec gets an
    ERROR-status ``CheckCollectionResult`` placeholder, later specs still run.

    Pins symmetric per-spec isolation between ``_execute_locally`` and
    ``_execute_on_agent`` — both paths build the same placeholder shape so
    consumers can iterate uniformly regardless of where the session ran.
    """
    from soda_core.check_collections.check_collection_verification import (
        ContractVerificationStatus,
    )

    def _good_verifier(**kwargs):
        return _SentinelAResult()

    def _broken_verifier(**kwargs):
        raise RuntimeError("simulated agent-side runtime failure")

    CheckCollection.register(
        kind="sentinel_a",
        yaml_class=_SentinelAYaml,
        impl_class=_SentinelAImpl,
        on_agent_verifier=_good_verifier,
    )
    CheckCollection.register(
        kind="sentinel_b",
        yaml_class=_SentinelBYaml,
        impl_class=_SentinelBImpl,
        on_agent_verifier=_broken_verifier,
    )

    specs = [
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_b", yaml_source=_yaml_source()),
        CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source()),
    ]

    result = CheckCollectionVerificationSessionImpl.execute(
        specs=specs,
        soda_cloud_use_agent=True,
        soda_cloud_publish_results=False,
    )

    assert len(result.check_collection_results) == 3
    assert isinstance(result.check_collection_results[0], _SentinelAResult)
    error_result = result.check_collection_results[1]
    assert isinstance(error_result, CheckCollectionResult)
    assert error_result.status is ContractVerificationStatus.ERROR
    assert isinstance(error_result._internal_exception, RuntimeError)
    assert isinstance(result.check_collection_results[2], _SentinelAResult)
