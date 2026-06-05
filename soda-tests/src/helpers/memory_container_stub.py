"""Helpers for building memory-container "stub" specs inside the container.

When a memory test uses ``memory_container(setup_outside=True)``, the host
creates the source table via ``__prepare_outside__`` (no cap). The container
then needs a ``TestTableSpecification`` to call ``ensure_test_table()`` —
but only to LOOK UP the already-existing table, not to recreate it. If the
in-container test builds the same full spec (with row data) just to compute
the unique_name, the row tuples become resident in memory and contaminate
the peak measurement by the full payload size.

This module lets the container skip that work. ``apply_container_overrides``
turns a builder configured with the same columns/purpose as the host into a
stub spec whose unique_name is read from the env-var bridge populated by
``memory_container_plugin._run_prepare_outside``. The stub keeps the right
row count (via a list of ``None`` placeholders) so ``verify_test_table_row_count``
still sees the correct length; the row CONTENT is discarded.

Default behaviour outside the container is unchanged — the helper detects
``SODA_IN_MEMORY_CONTAINER`` and only kicks in when the env var is set.
"""

from __future__ import annotations

import json
import os
from typing import Optional


_INNER_ENV_VAR = "SODA_IN_MEMORY_CONTAINER"
_FORCED_TABLE_NAMES_ENV = "SODA_MEMTEST_FORCED_TABLE_NAMES"


def in_memory_container() -> bool:
    """True iff we're running inside a memory_container docker exec."""
    return os.environ.get(_INNER_ENV_VAR) == "1"


def lookup_forced_table_name(table_purpose: str) -> Optional[str]:
    """Look up the host-built unique_name for the given table purpose.

    Returns None when not in a memory container, when the env var is absent,
    or when no ensured table matches the purpose. Matching is by case-insensitive
    substring on the unique_name (host names are formatted as
    ``SODATEST_<purpose>_<hash>``).
    """
    if not in_memory_container():
        return None
    raw = os.environ.get(_FORCED_TABLE_NAMES_ENV)
    if not raw:
        return None
    try:
        names = json.loads(raw)
    except (ValueError, TypeError):
        return None
    purpose_lower = table_purpose.lower()
    for name in names:
        if purpose_lower in name.lower():
            return name
    return None


def maybe_container_stub_spec(
    builder,
    table_purpose: str,
    expected_row_count: int,
):
    """If running inside a memory container AND the host already prepared a
    table whose unique_name matches ``table_purpose``, return a stub spec
    with the matching unique_name and ``[None] * expected_row_count`` for
    ``row_values``. Otherwise return None — caller should ``.build()`` the
    full spec normally.

    The builder is expected to already be configured with the same columns /
    table_purpose as the host-side spec. Row data on the builder is ignored
    when this returns non-None.
    """
    forced = lookup_forced_table_name(table_purpose)
    if forced is None:
        return None
    return builder.build(
        force_unique_name=forced,
        placeholder_row_count=expected_row_count,
    )
