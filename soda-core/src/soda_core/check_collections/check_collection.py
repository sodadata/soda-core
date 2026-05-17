"""Static descriptor for a check-collection subtype kind, used by the session orchestrator to dispatch per spec.

This module owns the ``CheckCollection`` dataclass — the *static descriptor*
that ties a wire ``kind`` to its YAML/impl class pair plus optional agent
verifier. The runtime instance for an executing check collection is
``CheckCollectionImpl`` (in ``check_collections.impl.check_collection_verification_impl``).
Following the codebase convention (``Contract`` / ``ContractImpl``), the
non-``Impl`` name is the static descriptor and the ``Impl`` name is the
runtime.

The registry API lives on the class itself as classmethods —
``CheckCollection.register(...)``, ``CheckCollection.get(kind)``,
``CheckCollection.all()``. There are no module-level free functions. The
registry storage is a class-private ``ClassVar`` dict.

Subtype packages call ``CheckCollection.register(...)`` at package-import
time (eager, single-source-of-truth). Tests that register sentinel
descriptors must snapshot ``CheckCollection._REGISTRY`` and restore it
after each test to avoid leaking entries across the session.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Optional, Protocol

from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml


class OnAgentVerifier(Protocol):
    """Descriptor hook that runs a check collection on the Soda Cloud agent.

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
    ) -> CheckCollectionResult:
        ...


@dataclass(frozen=True, kw_only=True)
class CheckCollection:
    """Static descriptor for a check-collection subtype kind.

    Each subtype package registers itself at module-import time. The session
    dispatcher looks up by ``kind`` to construct the right yaml + impl.

    Note: this is the *descriptor*, not a runtime instance. The runtime
    instance is ``CheckCollectionImpl``.

    The single source of truth for subtype identity (display name, wire
    source, test-mode scan-definition type, result class) lives on the
    impl class as ClassVars. The descriptor carries only what the dispatcher
    needs: the wire ``kind``, the YAML/impl class pair, and an optional
    agent verifier. Every consumer reads identity off
    ``descriptor.impl_class._DISPLAY_NAME`` etc. — duplicating those fields
    on the descriptor is a drift bug waiting to happen.
    """

    kind: str
    yaml_class: type[CheckCollectionYaml]
    impl_class: type[CheckCollectionImpl]
    on_agent_verifier: Optional[OnAgentVerifier]

    _REGISTRY: ClassVar[dict[str, "CheckCollection"]] = {}

    @classmethod
    def register(
        cls,
        *,
        kind: str,
        yaml_class: type[CheckCollectionYaml],
        impl_class: type[CheckCollectionImpl],
        on_agent_verifier: Optional[OnAgentVerifier] = None,
    ) -> None:
        """Register a check-collection subtype.

        Idempotent on identical re-register; raises ``ValueError`` on conflict.

        Validates at registration time that the impl class declares
        ``_WIRE_SOURCE`` (no silent fallback to the abstract base's empty slot)
        and that the YAML class's ``_KIND`` matches the registered ``kind``
        (no drift between the YAML discriminator and the dispatch key).
        """
        # Validate _WIRE_SOURCE is declared on the impl class — without this,
        # the first Cloud upload would silently fall through to AttributeError
        # at runtime instead of failing loudly at registration time.
        try:
            wire_source = impl_class._WIRE_SOURCE
        except AttributeError:
            raise ValueError(
                f"CheckCollection for kind {kind!r}: impl class " f"{impl_class.__name__} must declare _WIRE_SOURCE."
            )
        if not isinstance(wire_source, str) or not wire_source:
            raise ValueError(
                f"CheckCollection for kind {kind!r}: impl class "
                f"{impl_class.__name__}._WIRE_SOURCE must be a non-empty str, "
                f"got {wire_source!r}."
            )

        # Validate the YAML class's _KIND matches the registered kind. The YAML
        # discriminator is what callers write in ``kind:`` (or what spec.kind
        # carries); if it diverges from the registry key, dispatch would silently
        # route to the wrong descriptor on the next subtype.
        yaml_kind = getattr(yaml_class, "_KIND", None)
        if yaml_kind != kind:
            raise ValueError(
                f"CheckCollection for kind {kind!r}: yaml class "
                f"{yaml_class.__name__}._KIND is {yaml_kind!r}, must equal "
                f"the registered kind {kind!r}."
            )

        descriptor = cls(
            kind=kind,
            yaml_class=yaml_class,
            impl_class=impl_class,
            on_agent_verifier=on_agent_verifier,
        )

        existing = cls._REGISTRY.get(kind)
        if existing is None:
            cls._REGISTRY[kind] = descriptor
            return
        if existing == descriptor:
            return
        raise ValueError(
            f"CheckCollection for kind {kind!r} already registered with a different value. "
            f"Existing: {existing}. New: {descriptor}."
        )

    @classmethod
    def get(cls, kind: str) -> "CheckCollection":
        """Look up the descriptor for a kind. Raises ``KeyError`` with a helpful list on miss."""
        try:
            return cls._REGISTRY[kind]
        except KeyError:
            known = sorted(cls._REGISTRY)
            raise KeyError(f"No check-collection registered for kind {kind!r}. Known kinds: {known}.")

    @classmethod
    def all(cls) -> list["CheckCollection"]:
        """Return all registered descriptors in insertion order."""
        return list(cls._REGISTRY.values())
