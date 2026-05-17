"""Registry of check-collection subtype families used by the session orchestrator to dispatch per spec."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml


@dataclass(frozen=True)
class CheckCollectionFamily:
    """Everything the session orchestrator needs to verify one subtype.

    Each sibling subtype (``contract`` today, ``data_standard`` next phase)
    registers a family at import time. The session impl reads the spec's
    ``kind``, looks up the family, and dispatches.
    """

    kind: str
    display_name: str
    wire_source: str
    test_scan_definition_type: Optional[str]
    yaml_class: type[CheckCollectionYaml]
    impl_class: type[CheckCollectionImpl]
    result_class: type[CheckCollectionResult]
    on_agent_verifier: Optional[Callable[..., CheckCollectionResult]]


_REGISTRY: dict[str, CheckCollectionFamily] = {}


def register_family(family: CheckCollectionFamily) -> None:
    """Register a family. Idempotent on same-value re-registration; raises on conflict."""
    existing = _REGISTRY.get(family.kind)
    if existing is None:
        _REGISTRY[family.kind] = family
        return
    if existing == family:
        return
    raise ValueError(
        f"Family for kind {family.kind!r} already registered with a different value. "
        f"Existing: {existing}. New: {family}."
    )


def get_family(kind: str) -> CheckCollectionFamily:
    """Look up the family for a kind. Raises KeyError with a helpful list on miss."""
    try:
        return _REGISTRY[kind]
    except KeyError:
        known = sorted(_REGISTRY)
        raise KeyError(f"No check-collection family registered for kind {kind!r}. " f"Known kinds: {known}.")


def list_families() -> list[CheckCollectionFamily]:
    """Return all registered families in insertion order."""
    return list(_REGISTRY.values())
