"""Base-dialect seams for the all-datasource profiling/MM groundwork (OBSL-1036).

v3 parity contract:

- ``literal_timestamp_typed(dt)`` — typed timestamp literal for use INSIDE
  timestamp arithmetic. Base form = v3's ``sql_time_filter_to_timestamp``
  ``TIMESTAMP 'YYYY-MM-DD HH:MM:SS'`` (v3 data_source.py:1524-1529), incl.
  sub-second truncation. Must reproduce the MM bulk query builder's
  ``_timestamp_anchor_literal`` (soda-metric-monitoring
  bulk_query_builder.py:74-79) byte-for-byte. SQL Server family overrides
  with ``CAST('...' AS DATETIME2)`` — T-SQL has no TIMESTAMP '...' literal.

- ``sql_expr_is_not_nan(expr)`` — NaN-exclusion predicate for float
  aggregates; base None = no filter needed. Spark family overrides with
  ``NOT ISNAN({expr})`` (v3 spark_data_source.py:427-488).

- ``supports_percentile_within_group()`` — base True; Synapse overrides to
  False (v3 synapse_data_source.py:56-58 warned and returned a dummy 1; v4
  consumers skip the Q1/median/Q3 metrics instead).
"""

from __future__ import annotations

from datetime import datetime

from soda_core.common.sql_dialect import SqlDialect


def dialect() -> SqlDialect:
    return SqlDialect()


# ---------------------------------------------------------------------------
# literal_timestamp_typed — v3 sql_time_filter_to_timestamp form
# ---------------------------------------------------------------------------


def test_literal_timestamp_typed_base_form():
    assert dialect().literal_timestamp_typed(datetime(2020, 6, 20, 0, 0, 0)) == "TIMESTAMP '2020-06-20 00:00:00'"


def test_literal_timestamp_typed_truncates_sub_seconds():
    """v3 strftime('%Y-%m-%d %H:%M:%S') drops microseconds — kept verbatim."""
    assert dialect().literal_timestamp_typed(datetime(2025, 1, 2, 3, 4, 5, 999999)) == "TIMESTAMP '2025-01-02 03:04:05'"


def test_literal_timestamp_typed_pads_components():
    assert dialect().literal_timestamp_typed(datetime(2025, 1, 2, 3, 4, 5)) == "TIMESTAMP '2025-01-02 03:04:05'"


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
