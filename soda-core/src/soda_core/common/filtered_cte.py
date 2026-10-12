"""The filtered-dataset CTE that check-collection queries select from, and its aliases.

The base scope reads ``SODA_FILTERED_CTE_NAME``. Each declared scope gets its own alias
from ``filtered_cte_alias``, so two scopes never share a CTE. Nothing here imports from
``soda_core.contracts``; the base scope is passed as ``None``.
"""

from __future__ import annotations

import re
from hashlib import blake2b
from numbers import Number
from typing import Optional

from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.metadata_types import SamplerType
from soda_core.common.sql_ast import CTE, FROM, SELECT, SODA_FILTERED_CTE_NAME, STAR, WHERE, SqlExpressionStr

FILTERED_SCOPE_CTE_PREFIX: str = "_soda_filtered_scope_"
# Oracle caps identifiers at 30 bytes. Postgres allows 63 and SQL Server 128.
FILTERED_CTE_ALIAS_MAX_LENGTH: int = 30
_FILTERED_CTE_ALIAS_PREFIXES: tuple[str, ...] = (
    "_soda_filtered_source_",
    "_soda_filtered_target_",
    FILTERED_SCOPE_CTE_PREFIX,
)


def build_filtered_cte(
    dataset_identifier: DatasetIdentifier,
    filter: Optional[str],
    alias: str,
    sampler: Optional[tuple[SamplerType, Number]] = None,
) -> CTE:
    """``SELECT * FROM <dataset> [WHERE <filter>]`` under ``alias``, sampled when a sampler is given.

    The caller decides whether to sample. A scope filter replaces the top-level filter; it is
    never ANDed with it.
    """
    cte: CTE = CTE(alias).AS(
        [
            SELECT(STAR()),
            FROM(dataset_identifier.dataset_name, dataset_identifier.prefixes),
            WHERE.optional(SqlExpressionStr.optional(filter)),
        ]
    )
    if sampler is not None:
        cte.cte_query[1] = cte.cte_query[1].SAMPLE(*sampler)
    return cte


def filtered_cte_alias(key: Optional[str]) -> str:
    """The CTE alias for a scope key, ``None`` for the base scope.

    A key that sanitises to itself and fits the length cap is used as is. Any other key
    is replaced by a hash of the original, so ``eu-west`` and ``eu_west`` never share an
    alias. A plain alias has a letter right after the prefix and a hashed one has ``_``,
    so the two forms never meet. The prefix leaves no room for part of the key next to the
    digest under the cap, so a hashed alias carries the digest only.
    """
    if key is None:
        return SODA_FILTERED_CTE_NAME
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", key)
    alias = f"{FILTERED_SCOPE_CTE_PREFIX}{sanitized}"
    if sanitized == key and len(alias) <= FILTERED_CTE_ALIAS_MAX_LENGTH:
        return alias
    digest = blake2b(key.encode("utf-8"), digest_size=4).hexdigest()
    return f"{FILTERED_SCOPE_CTE_PREFIX}_{digest}"


def is_filtered_cte_alias(alias: str) -> bool:
    """Whether ``alias`` names a filtered-dataset CTE: the base alias exactly, or a
    reconciliation source, reconciliation target or scope alias by prefix. Case-sensitive."""
    return alias == SODA_FILTERED_CTE_NAME or alias.startswith(_FILTERED_CTE_ALIAS_PREFIXES)
