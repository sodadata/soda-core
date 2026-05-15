"""Process-wide registry of snapshot interceptors.

A snapshot interceptor is anything that monkey-patches ``DataSourceImpl``
methods to wire a ``SnapshotDataSourceConnection`` into a code path that
normally opens a plain connection — currently only soda-extensions'
``DwhSnapshotInterceptor`` (which wraps DWH connections), but designed so
future extensions can plug in the same way.

The pytest rerun plugin needs to deactivate any active interceptor before
firing a rerun so the rerun's test body runs against a plain real
connection. Earlier revisions of the plugin did this by reflecting on a
specific class attribute of ``DwhSnapshotInterceptor``::

    from test_helpers.dwh_setup import DwhSnapshotInterceptor
    inst = getattr(DwhSnapshotInterceptor, "_active_instance", None)
    if inst is not None:
        inst.deactivate()

That had two problems:

1. **Silent no-op on rename.** The import is best-effort; if soda-extensions
   ever renames the module or the attribute, the plugin's deactivation step
   silently disappears and surface as a "stale interceptor" symptom in
   downstream tests, not a clear failure.
2. **Doesn't scale.** Every future thing that needs to know about reruns
   has to be hard-coded into the same try/except block.

The registry replaces that reflection with a duck-typed protocol. Interceptors
register themselves on activate, unregister on deactivate, and the plugin
walks the live set. Mirrors the design of ``_LIVE_SNAPSHOT_WRAPPERS``.
"""

from __future__ import annotations

import logging
import weakref
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class SnapshotInterceptor(Protocol):
    """Minimal contract for anything the rerun plugin needs to deactivate
    before re-running a test against the real DB.

    The protocol is intentionally tiny: deactivate must be idempotent and
    must restore whichever ``DataSourceImpl`` method(s) the interceptor
    patched so that the subsequent rerun sees the original behaviour.
    """

    def deactivate(self) -> None:
        ...


# WeakSet so a forgotten interceptor (e.g. one orphaned by a test that
# crashed before reaching teardown) doesn't keep the object alive past
# its natural lifetime. Walking the set during a rerun yields whatever's
# still reachable.
_REGISTERED_INTERCEPTORS: "weakref.WeakSet[SnapshotInterceptor]" = weakref.WeakSet()


def register_snapshot_interceptor(interceptor: SnapshotInterceptor) -> None:
    """Add ``interceptor`` to the process-wide registry.

    Called by an interceptor's ``activate`` method. Idempotent — re-adding
    an already-registered instance is a no-op.
    """
    _REGISTERED_INTERCEPTORS.add(interceptor)


def unregister_snapshot_interceptor(interceptor: SnapshotInterceptor) -> None:
    """Remove ``interceptor`` from the registry.

    Called by an interceptor's ``deactivate`` method. Silent if the
    interceptor was never registered (or has already been GC'd).
    """
    _REGISTERED_INTERCEPTORS.discard(interceptor)


def deactivate_all_registered_interceptors() -> None:
    """Best-effort: call ``deactivate()`` on every currently-registered
    interceptor. Used by the pytest rerun plugin between the failed first
    attempt and the rerun so no stale wrapping survives into the rerun.

    Each deactivate is wrapped in its own try/except — a misbehaving
    interceptor cannot prevent others from being deactivated.
    """
    # Materialise to a list first so deactivate() implementations that
    # unregister themselves don't mutate the set while we're iterating.
    for interceptor in list(_REGISTERED_INTERCEPTORS):
        try:
            interceptor.deactivate()
        except Exception as exc:
            logger.warning(f"SNAPSHOT: failed to deactivate registered interceptor {interceptor!r}: {exc!r}")
