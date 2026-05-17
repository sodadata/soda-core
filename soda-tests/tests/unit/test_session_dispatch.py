"""Pins the universal session impl's dispatch behavior over the family
registry: mixed-kind sessions aggregate per-spec results in input order,
unknown kinds raise a clear lookup error, and the agent path raises a
clear ``NotImplementedError`` for families registered with
``on_agent_verifier=None``.

Replaces the session-side Generic-derivation tests that were dropped when
``CheckCollectionVerificationSessionImpl`` stopped subclassing
``Generic[YamlT, ImplT, ResultT]`` — dispatch is now per-spec via the
registry rather than via a session subclass.
"""

from __future__ import annotations

from typing import Iterator

import pytest
from soda_core.check_collections.check_collection_family import (
    _REGISTRY,
    CheckCollectionFamily,
    register_family,
)
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

# Two sentinel families register at test time. We snapshot/restore the
# module-level registry so they don't leak across tests.


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
    """Snapshot the registry around each test so sentinel families don't leak."""
    saved = dict(_REGISTRY)
    try:
        yield
    finally:
        _REGISTRY.clear()
        _REGISTRY.update(saved)


def _yaml_source() -> CheckCollectionYamlSource:
    return CheckCollectionYamlSource.from_str(_MINIMAL_YAML)


def _register_sentinel_a(*, on_agent_verifier=None) -> None:
    register_family(
        CheckCollectionFamily(
            kind="sentinel_a",
            yaml_class=_SentinelAYaml,
            impl_class=_SentinelAImpl,
            on_agent_verifier=on_agent_verifier,
        )
    )


def _register_sentinel_b(*, on_agent_verifier=None) -> None:
    register_family(
        CheckCollectionFamily(
            kind="sentinel_b",
            yaml_class=_SentinelBYaml,
            impl_class=_SentinelBImpl,
            on_agent_verifier=on_agent_verifier,
        )
    )


def test_mixed_kind_session_aggregates_per_spec_results_in_order():
    """A heterogeneous ``specs=`` list dispatches each spec to its family and
    aggregates the per-spec results in input order on the session result."""
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


def test_unknown_kind_raises_with_known_kinds_list():
    """A spec referencing an unregistered ``kind`` bubbles a ``KeyError``
    out of the session impl that mentions the kind and the known list."""
    _register_sentinel_a()

    spec = CheckCollectionSpec(kind="unregistered", yaml_source=_yaml_source())

    with pytest.raises(KeyError) as exc_info:
        CheckCollectionVerificationSessionImpl.execute(
            specs=[spec],
            only_validate_without_execute=True,
            soda_cloud_publish_results=False,
            soda_cloud_use_agent=False,
        )
    message = str(exc_info.value)
    assert "unregistered" in message
    assert "Known kinds:" in message


def test_agent_path_with_on_agent_verifier_none_raises_not_implemented():
    """A family registered with ``on_agent_verifier=None`` cleanly rejects
    agent dispatch with a message that names the family's display name and
    mentions agent execution."""
    _register_sentinel_a(on_agent_verifier=None)

    spec = CheckCollectionSpec(kind="sentinel_a", yaml_source=_yaml_source())

    with pytest.raises(NotImplementedError) as exc_info:
        CheckCollectionVerificationSessionImpl.execute(
            specs=[spec],
            soda_cloud_use_agent=True,
            soda_cloud_publish_results=False,
        )
    message = str(exc_info.value)
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

    register_family(
        CheckCollectionFamily(
            kind="sentinel_a",
            yaml_class=_SentinelAYaml,
            impl_class=_CapturingSentinelAImpl,
            on_agent_verifier=None,
        )
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
    a plain ``RuntimeError`` (simulating an unexpected runtime bug, not a
    caller-input error). The result list has length 3 with ``None`` in the
    middle slot and real results at the edges. Consumers (e.g. backend
    ingestion) may match results to inputs by index, so the positional
    contract matters.

    Note: ``SodaCoreException`` and its subclasses (YamlParserException etc.)
    are *not* isolated — they bubble out to match the legacy
    pytest.raises(YamlParserException) contract that callers pin in tests
    like test_contract_parsing_errors.
    """
    # Per-test sentinel impl that raises during verify(). We register a fresh
    # family with this impl so the failure is genuinely per-spec.
    class _BrokenSentinelImpl(_SentinelAImpl):
        _WIRE_SOURCE = "soda-sentinel-broken"

        def verify(self):
            raise RuntimeError("simulated unexpected runtime failure")

    class _BrokenSentinelYaml(_SentinelAYaml):
        _KIND = "sentinel_broken"

    register_family(
        CheckCollectionFamily(
            kind="sentinel_broken",
            yaml_class=_BrokenSentinelYaml,
            impl_class=_BrokenSentinelImpl,
            on_agent_verifier=None,
        )
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
    # Positional: edges are real results, middle is the failure sentinel.
    assert isinstance(result.check_collection_results[0], _SentinelAResult)
    assert result.check_collection_results[1] is None
    assert isinstance(result.check_collection_results[2], _SentinelAResult)
