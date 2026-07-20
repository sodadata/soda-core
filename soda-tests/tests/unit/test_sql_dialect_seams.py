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
