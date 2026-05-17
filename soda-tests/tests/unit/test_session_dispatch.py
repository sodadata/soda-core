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
    pass


class _SentinelAResult(CheckCollectionResult):
    pass


class _SentinelAImpl(CheckCollectionImpl[_SentinelAYaml, _SentinelAResult]):
    _WIRE_SOURCE = "soda-sentinel-a"


class _SentinelBYaml(CheckCollectionYaml):
    pass


class _SentinelBResult(CheckCollectionResult):
    pass


class _SentinelBImpl(CheckCollectionImpl[_SentinelBYaml, _SentinelBResult]):
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
            display_name="sentinel-a",
            wire_source="soda-sentinel-a",
            test_scan_definition_type=None,
            yaml_class=_SentinelAYaml,
            impl_class=_SentinelAImpl,
            result_class=_SentinelAResult,
            on_agent_verifier=on_agent_verifier,
        )
    )


def _register_sentinel_b(*, on_agent_verifier=None) -> None:
    register_family(
        CheckCollectionFamily(
            kind="sentinel_b",
            display_name="sentinel-b",
            wire_source="soda-sentinel-b",
            test_scan_definition_type=None,
            yaml_class=_SentinelBYaml,
            impl_class=_SentinelBImpl,
            result_class=_SentinelBResult,
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
    agent dispatch with a message that names the family's display_name and
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
