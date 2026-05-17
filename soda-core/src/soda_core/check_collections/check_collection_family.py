"""Registry of check-collection subtype families used by the session orchestrator to dispatch per spec.

The registry is module-level mutable state. Subtype packages register their
family at package-import time (eager, single-source-of-truth). Tests that
register sentinel families must snapshot ``_REGISTRY`` and restore it after
each test to avoid leaking entries across the session.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol

from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml


class OnAgentVerifier(Protocol):
    """Family hook that runs a check collection on the Soda Cloud agent.

    Matches the call site in
    ``CheckCollectionVerificationSessionImpl._execute_on_agent``: all kwargs
    are passed by name, optional kwargs are passed as ``None`` when the
    caller has nothing to forward.
    """

    def __call__(
        self,
        *,
        soda_cloud_impl: object,  # SodaCloud; declared as object to avoid an import cycle
        check_collection_yaml: CheckCollectionYaml,
        variables: Optional[dict[str, str]],
        blocking_timeout_in_minutes: Optional[int],
        publish_results: bool,
        verbose: bool,
    ) -> CheckCollectionResult: ...


@dataclass(frozen=True, kw_only=True)
class CheckCollectionFamily:
    """Everything the session orchestrator needs to dispatch one subtype.

    The single source of truth for subtype identity (display name, wire
    source, test-mode scan-definition type, result class) lives on the
    impl class as ClassVars. The family carries only what the dispatcher
    needs: the wire ``kind``, the YAML/impl class pair, and an optional
    agent verifier. Every consumer reads identity off
    ``family.impl_class._DISPLAY_NAME`` etc. — duplicating those fields
    on the family is a drift bug waiting to happen.
    """

    kind: str
    yaml_class: type[CheckCollectionYaml]
    impl_class: type[CheckCollectionImpl]
    on_agent_verifier: Optional[OnAgentVerifier]


_REGISTRY: dict[str, CheckCollectionFamily] = {}


def register_family(family: CheckCollectionFamily) -> None:
    """Register a family. Idempotent on same-value re-registration; raises on conflict.

    Validates at registration time that the impl class declares
    ``_WIRE_SOURCE`` (no silent fallback to the abstract base's empty slot)
    and that the YAML class's ``_KIND`` matches the registered ``kind``
    (no drift between the YAML discriminator and the dispatch key).
    """
    # Validate _WIRE_SOURCE is declared on the impl class — without this,
    # the first Cloud upload would silently fall through to AttributeError
    # at runtime instead of failing loudly at family-registration time.
    try:
        wire_source = family.impl_class._WIRE_SOURCE
    except AttributeError:
        raise ValueError(
            f"CheckCollectionFamily for kind {family.kind!r}: impl class "
            f"{family.impl_class.__name__} must declare _WIRE_SOURCE."
        )
    if not isinstance(wire_source, str) or not wire_source:
        raise ValueError(
            f"CheckCollectionFamily for kind {family.kind!r}: impl class "
            f"{family.impl_class.__name__}._WIRE_SOURCE must be a non-empty str, "
            f"got {wire_source!r}."
        )

    # Validate the YAML class's _KIND matches the registered kind. The YAML
    # discriminator is what callers write in ``kind:`` (or what spec.kind
    # carries); if it diverges from the registry key, dispatch would silently
    # route to the wrong family on the next subtype.
    yaml_kind = getattr(family.yaml_class, "_KIND", None)
    if yaml_kind != family.kind:
        raise ValueError(
            f"CheckCollectionFamily for kind {family.kind!r}: yaml class "
            f"{family.yaml_class.__name__}._KIND is {yaml_kind!r}, must equal "
            f"the registered kind {family.kind!r}."
        )

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
        raise KeyError(f"No check-collection family registered for kind {kind!r}. Known kinds: {known}.")


def list_families() -> list[CheckCollectionFamily]:
    """Return all registered families in insertion order."""
    return list(_REGISTRY.values())
