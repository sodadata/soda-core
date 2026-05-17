"""Pins behavior of the family registry: register, get, list, idempotence, conflict, unknown kind."""

from __future__ import annotations

from typing import Iterator

import pytest
from soda_core.check_collections.check_collection_family import (
    _REGISTRY,
    CheckCollectionFamily,
    get_family,
    list_families,
    register_family,
)
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

    The registry is module-level mutable state; tests must not leak entries
    into each other or into other test modules in the same session.
    """
    saved = dict(_REGISTRY)
    _REGISTRY.clear()
    try:
        yield
    finally:
        _REGISTRY.clear()
        _REGISTRY.update(saved)


# Stand-in classes for family fixtures. Only their class identity matters.


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


def _make_family(
    kind: str = "sentinel",
    *,
    yaml_class: type[CheckCollectionYaml] = _SentinelYaml,
    impl_class: type[CheckCollectionImpl] = _SentinelImpl,
) -> CheckCollectionFamily:
    return CheckCollectionFamily(
        kind=kind,
        yaml_class=yaml_class,
        impl_class=impl_class,
        on_agent_verifier=None,
    )


def test_register_and_get_round_trip():
    """register_family + get_family(kind) returns the same family."""
    family = _make_family(kind="sentinel")
    register_family(family)
    assert get_family("sentinel") is family


def test_list_families_returns_all_registered():
    """list_families() returns every registered family."""
    sentinel = _make_family(kind="sentinel")
    other = _make_family(
        kind="other",
        yaml_class=_OtherYaml,
        impl_class=_OtherImpl,
    )
    register_family(sentinel)
    register_family(other)

    families = list_families()
    assert sentinel in families
    assert other in families
    assert len(families) == 2


def test_idempotent_same_value_reregister():
    """Re-registering the same family is a no-op (no raise, no duplicate)."""
    family = _make_family(kind="sentinel")
    register_family(family)
    register_family(family)  # second call must not raise

    assert get_family("sentinel") is family
    assert len(list_families()) == 1


def test_conflict_raises_value_error():
    """Re-registering a different family for the same kind raises ValueError."""
    first = _make_family(kind="sentinel")
    register_family(first)

    different = CheckCollectionFamily(
        kind="sentinel",
        yaml_class=_SentinelYaml,
        impl_class=_OtherImpl,  # differs from `first` impl
        on_agent_verifier=None,
    )
    with pytest.raises(ValueError, match="sentinel") as exc_info:
        register_family(different)
    assert "different value" in str(exc_info.value)


def test_unknown_kind_raises_key_error_with_known_list():
    """get_family on an unregistered kind raises KeyError mentioning 'Known kinds:'."""
    register_family(_make_family(kind="sentinel"))

    with pytest.raises(KeyError) as exc_info:
        get_family("nope")
    message = str(exc_info.value)
    assert "Known kinds:" in message
    assert "nope" in message


def test_empty_registry_get_says_known_kinds_empty_and_list_is_empty():
    """Empty registry: get raises with 'Known kinds: []' and list_families() returns []."""
    assert list_families() == []
    with pytest.raises(KeyError) as exc_info:
        get_family("anything")
    assert "Known kinds: []" in str(exc_info.value)
