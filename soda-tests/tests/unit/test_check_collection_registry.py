"""Pins behavior of the CheckCollection registry: register, get, all, idempotence, conflict, unknown kind."""

from __future__ import annotations

from typing import Iterator

import pytest
from soda_core.check_collections.check_collection import CheckCollection
from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml


@pytest.fixture(autouse=True)
def _clean_registry() -> Iterator[None]:
    """Snapshot the registry before each test and restore after.

    The registry is class-level mutable state on ``CheckCollection``; tests
    must not leak entries into each other or into other test modules in the
    same session.
    """
    saved = dict(CheckCollection._REGISTRY)
    CheckCollection._REGISTRY.clear()
    try:
        yield
    finally:
        CheckCollection._REGISTRY.clear()
        CheckCollection._REGISTRY.update(saved)


# Stand-in classes for descriptor fixtures. Only their class identity matters.


class _SentinelYaml(CheckCollectionYaml):
    _KIND = "sentinel"


class _SentinelResult(CheckCollectionResult):
    pass


class _SentinelImpl(CheckCollectionImpl[_SentinelYaml, _SentinelResult]):
    _WIRE_SOURCE = "soda-sentinel"


class _OtherYaml(CheckCollectionYaml):
    _KIND = "other"


class _OtherResult(CheckCollectionResult):
    pass


class _OtherImpl(CheckCollectionImpl[_OtherYaml, _OtherResult]):
    _WIRE_SOURCE = "soda-other"


def _register_sentinel(
    kind: str = "sentinel",
    *,
    yaml_class: type[CheckCollectionYaml] = _SentinelYaml,
    impl_class: type[CheckCollectionImpl] = _SentinelImpl,
) -> CheckCollection:
    CheckCollection.register(
        kind=kind,
        yaml_class=yaml_class,
        impl_class=impl_class,
        on_agent_verifier=None,
    )
    return CheckCollection.get(kind)


def test_register_and_get_round_trip():
    """CheckCollection.register + CheckCollection.get(kind) returns the registered descriptor."""
    descriptor = _register_sentinel(kind="sentinel")
    assert CheckCollection.get("sentinel") is descriptor


def test_all_returns_every_registered_descriptor():
    """CheckCollection.all() returns every registered descriptor."""
    sentinel = _register_sentinel(kind="sentinel")
    other = _register_sentinel(
        kind="other",
        yaml_class=_OtherYaml,
        impl_class=_OtherImpl,
    )

    descriptors = CheckCollection.all()
    assert sentinel in descriptors
    assert other in descriptors
    assert len(descriptors) == 2


def test_idempotent_same_value_reregister():
    """Re-registering with identical fields is a no-op (no raise, no duplicate)."""
    descriptor = _register_sentinel(kind="sentinel")
    # second call must not raise
    CheckCollection.register(
        kind="sentinel",
        yaml_class=_SentinelYaml,
        impl_class=_SentinelImpl,
        on_agent_verifier=None,
    )

    assert CheckCollection.get("sentinel") == descriptor
    assert len(CheckCollection.all()) == 1


def test_conflict_raises_value_error():
    """Re-registering different fields for the same kind raises ValueError."""
    _register_sentinel(kind="sentinel")

    with pytest.raises(ValueError, match="sentinel") as exc_info:
        CheckCollection.register(
            kind="sentinel",
            yaml_class=_SentinelYaml,
            impl_class=_OtherImpl,  # differs from first registration's impl
            on_agent_verifier=None,
        )
    assert "different value" in str(exc_info.value)


def test_unknown_kind_raises_key_error_with_known_list():
    """CheckCollection.get on an unregistered kind raises KeyError mentioning 'Known kinds:'."""
    _register_sentinel(kind="sentinel")

    with pytest.raises(KeyError) as exc_info:
        CheckCollection.get("nope")
    message = str(exc_info.value)
    assert "Known kinds:" in message
    assert "nope" in message


def test_empty_registry_get_says_known_kinds_empty_and_all_is_empty():
    """Empty registry: get raises with 'Known kinds: []' and all() returns []."""
    assert CheckCollection.all() == []
    with pytest.raises(KeyError) as exc_info:
        CheckCollection.get("anything")
    assert "Known kinds: []" in str(exc_info.value)


def test_register_rejects_impl_with_empty_wire_source():
    """``CheckCollection.register`` validates that the impl class declares a
    non-empty ``_WIRE_SOURCE`` string. An empty value would silently route the
    Cloud upload under an empty ``source`` literal and the backend would drop
    the checks. The validator must fail loud at registration time, naming the
    impl class and explaining the requirement.
    """

    class _SentinelEmptyWireYaml(CheckCollectionYaml):
        _KIND = "sentinel_empty_wire"

    class _SentinelEmptyWireResult(CheckCollectionResult):
        pass

    class _SentinelEmptyWireImpl(CheckCollectionImpl[_SentinelEmptyWireYaml, _SentinelEmptyWireResult]):
        _WIRE_SOURCE = ""

    with pytest.raises(ValueError) as exc_info:
        CheckCollection.register(
            kind="sentinel_empty_wire",
            yaml_class=_SentinelEmptyWireYaml,
            impl_class=_SentinelEmptyWireImpl,
            on_agent_verifier=None,
        )
    message = str(exc_info.value)
    assert "_WIRE_SOURCE" in message
    assert "non-empty str" in message
    assert "_SentinelEmptyWireImpl" in message


def test_register_rejects_impl_with_non_str_wire_source():
    """``CheckCollection.register`` rejects a non-str ``_WIRE_SOURCE`` (e.g. an
    int). The validator must fail loud at registration time with the same
    'non-empty str' error class & message.
    """

    class _SentinelIntWireYaml(CheckCollectionYaml):
        _KIND = "sentinel_int_wire"

    class _SentinelIntWireResult(CheckCollectionResult):
        pass

    class _SentinelIntWireImpl(CheckCollectionImpl[_SentinelIntWireYaml, _SentinelIntWireResult]):
        _WIRE_SOURCE = 42  # type: ignore[assignment]

    with pytest.raises(ValueError) as exc_info:
        CheckCollection.register(
            kind="sentinel_int_wire",
            yaml_class=_SentinelIntWireYaml,
            impl_class=_SentinelIntWireImpl,
            on_agent_verifier=None,
        )
    message = str(exc_info.value)
    assert "_WIRE_SOURCE" in message
    assert "non-empty str" in message
    assert "_SentinelIntWireImpl" in message


def test_register_rejects_kind_yaml_mismatch():
    """``CheckCollection.register`` validates that the YAML class's ``_KIND``
    matches the registered ``kind`` arg. A mismatch means the YAML
    discriminator is wired to a different dispatch slot than the registry
    expects — dispatch on the next subtype would silently route to the wrong
    descriptor. Validator must fail loud at registration time naming both
    sides of the mismatch.
    """

    class _SentinelMismatchYaml(CheckCollectionYaml):
        _KIND = "foo_yaml_kind"

    class _SentinelMismatchResult(CheckCollectionResult):
        pass

    class _SentinelMismatchImpl(CheckCollectionImpl[_SentinelMismatchYaml, _SentinelMismatchResult]):
        _WIRE_SOURCE = "soda-sentinel-mismatch"

    with pytest.raises(ValueError) as exc_info:
        CheckCollection.register(
            kind="foo_register_kind",
            yaml_class=_SentinelMismatchYaml,
            impl_class=_SentinelMismatchImpl,
            on_agent_verifier=None,
        )
    message = str(exc_info.value)
    assert "foo_register_kind" in message
    assert "foo_yaml_kind" in message
    assert "_SentinelMismatchYaml" in message
