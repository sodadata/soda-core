"""Base-dialect seams for the all-datasource profiling/MM groundwork.

- ``literal_timestamp_typed(dt)`` — typed timestamp literal for use inside
  timestamp arithmetic: ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS'``, sub-seconds
  truncated. Must match the MM bulk query builder's anchor literal
  byte-for-byte. The SQL Server family overrides with
  ``CAST('...' AS DATETIME2)`` — T-SQL has no TIMESTAMP '...' literal.

- ``sql_expr_is_not_nan(expr)`` — NaN-exclusion predicate for float
  aggregates; base None = no filter needed. The Spark family overrides with
  ``NOT ISNAN({expr})``.

- ``supports_percentile_within_group()`` — base True; Synapse overrides to
  False (no percentile aggregate) and consumers skip the Q1/median/Q3
  metrics.
"""

from __future__ import annotations

from datetime import datetime

from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_dialect import SqlDialect


def dialect() -> SqlDialect:
    return SqlDialect()


# ---------------------------------------------------------------------------
# literal_timestamp_typed
# ---------------------------------------------------------------------------


def test_literal_timestamp_typed_base_form():
    assert dialect().literal_timestamp_typed(datetime(2020, 6, 20, 0, 0, 0)) == "TIMESTAMP '2020-06-20 00:00:00'"


def test_literal_timestamp_typed_truncates_sub_seconds():
    """Microseconds are dropped, not rounded."""
    assert dialect().literal_timestamp_typed(datetime(2025, 1, 2, 3, 4, 5, 999999)) == "TIMESTAMP '2025-01-02 03:04:05'"


def test_literal_timestamp_typed_pads_components():
    assert dialect().literal_timestamp_typed(datetime(2025, 1, 2, 3, 4, 5)) == "TIMESTAMP '2025-01-02 03:04:05'"


def test_literal_timestamp_typed_normalizes_tz_aware_to_utc():
    """A tz-aware datetime is converted to UTC before rendering; strftime alone
    would drop tzinfo and emit local wall-clock against a UTC-typed column."""
    from datetime import timedelta, timezone

    plus_two = timezone(timedelta(hours=2))
    assert (
        dialect().literal_timestamp_typed(datetime(2025, 1, 2, 5, 4, 5, tzinfo=plus_two))
        == "TIMESTAMP '2025-01-02 03:04:05'"
    )


# ---------------------------------------------------------------------------
# sql_expr_is_not_nan — base has no NaN values to filter
# ---------------------------------------------------------------------------


def test_sql_expr_is_not_nan_base_is_none():
    assert dialect().sql_expr_is_not_nan('"c"') is None


# ---------------------------------------------------------------------------
# supports_percentile_within_group — base True
# ---------------------------------------------------------------------------


def test_supports_percentile_within_group_base_is_true():
    assert dialect().supports_percentile_within_group() is True


# ---------------------------------------------------------------------------
# select_all_paginated_sql — the cross-source recon key-normalization seam.
# Default-off (empty normalize set) must render byte-for-byte prior SQL; a
# flagged key column is case-folded with LOWER() so a case-insensitive peer
# (e.g. Salesforce/SOQL) and this SQL source order text the same way. This is
# the OSS seam the extension recon feature depends on — pin it soda-core-side.
# ---------------------------------------------------------------------------


def _paginated(order_by, normalize_key_columns=frozenset()):
    return dialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=["s"], dataset_name="t"),
        columns=["code", "label"],
        filter=None,
        order_by=order_by,
        limit=10,
        offset=0,
        normalize_key_columns=normalize_key_columns,
    )


def test_paginated_sql_default_off_is_byte_identical():
    # M4: pin the exact default-path SQL so the "byte-for-byte unchanged when normalize is empty"
    # guarantee survives refactors — not just an absence-of-LOWER check.
    assert _paginated(order_by=["code"]) == (
        'SELECT "code",\n       "label"\nFROM "s"."t"\n' 'ORDER BY "code" ASC\nLIMIT 10\nOFFSET 0;'
    )


def test_paginated_sql_normalizes_flagged_key_with_deterministic_tiebreaker():
    # A flagged key is case-folded (LOWER, no spurious parens — via the AST node), THEN the raw
    # column is appended as a deterministic tiebreaker so LIMIT/OFFSET paging is stable for
    # case-colliding keys; the co-ordered non-flagged "label" stays raw.
    assert _paginated(order_by=["code", "label"], normalize_key_columns=frozenset({"code"})) == (
        'SELECT "code",\n       "label"\nFROM "s"."t"\n'
        'ORDER BY LOWER("code") ASC, "code" ASC, "label" ASC\nLIMIT 10\nOFFSET 0;'
    )


def test_order_by_key_expression_returns_a_lower_ast_expression():
    # C3: the fold is an AST LOWER node (rendered per-dialect via _build_lower_sql), not an f-string.
    d = dialect()
    assert d.build_expression_sql(d.order_by_key_expression("c")) == 'LOWER("c")'


def test_supports_row_sampling_base_is_true():
    # The recon sampling fail-loud guard fires only when this is False; base SQL sources must
    # report True so a sampled recon on them is never flipped to NOT_EVALUATED.
    assert dialect().supports_row_sampling() is True
