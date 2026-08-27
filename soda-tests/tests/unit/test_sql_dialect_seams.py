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

import pytest
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_ast import ALIAS, SqlExpressionStr
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


def _paginated(order_by, normalize_key_columns=frozenset(), distinct=False, columns=("code", "label")):
    return dialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=["s"], dataset_name="t"),
        columns=list(columns),
        filter=None,
        order_by=order_by,
        limit=10,
        offset=0,
        normalize_key_columns=normalize_key_columns,
        distinct=distinct,
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


# ---------------------------------------------------------------------------
# distinct x normalize_key_columns. Separately, both render a flat select. Together
# they cannot: SQL requires every ORDER BY term of a `SELECT DISTINCT` to appear in
# its select list, and the LOWER() fold is deliberately not projected (callers read
# the page's rows positionally). The composed shape de-duplicates in a CTE and
# orders/paginates that instead.
# ---------------------------------------------------------------------------


def test_paginated_sql_distinct_only_stays_flat():
    assert _paginated(order_by=["code"], distinct=True) == (
        'SELECT DISTINCT "code",\n       "label"\nFROM "s"."t"\n' 'ORDER BY "code" ASC\nLIMIT 10\nOFFSET 0;'
    )


def test_paginated_sql_distinct_with_normalized_key_dedups_in_a_cte():
    assert _paginated(order_by=["code"], normalize_key_columns=frozenset({"code"}), distinct=True) == (
        'WITH \n"_soda_distinct_page" AS (\n'
        'SELECT DISTINCT "code",\n       "label"\nFROM "s"."t"\n'
        ")\n"
        'SELECT "code",\n       "label"\nFROM "_soda_distinct_page"\n'
        'ORDER BY LOWER("code") ASC, "code" ASC\nLIMIT 10\nOFFSET 0;'
    )


def test_paginated_sql_distinct_with_normalized_key_projects_nothing_extra():
    """The de-duplicating CTE and the outer select project exactly `columns` — a caller reading
    the page positionally must not receive the sort key as an extra field."""
    sql = _paginated(order_by=["code"], normalize_key_columns=frozenset({"code"}), distinct=True)
    assert sql.count("SELECT") == 2
    assert sql.count('"label"') == 2  # once per projection, never in the ORDER BY
    assert sql.count("LOWER(") == 1  # only in the ORDER BY, never projected


def test_paginated_sql_distinct_with_normalized_key_without_explicit_columns():
    """`SELECT DISTINCT *` projects every column, so the fold always resolves against the CTE."""
    assert _paginated(order_by=["code"], normalize_key_columns=frozenset({"code"}), distinct=True, columns=()) == (
        'WITH \n"_soda_distinct_page" AS (\n'
        'SELECT DISTINCT *\nFROM "s"."t"\n'
        ")\n"
        'SELECT *\nFROM "_soda_distinct_page"\n'
        'ORDER BY LOWER("code") ASC, "code" ASC\nLIMIT 10\nOFFSET 0;'
    )


def test_paginated_sql_distinct_rejects_an_unprojected_order_by_column():
    """Fail with the offending column instead of emitting SQL that Postgres/T-SQL/BigQuery reject
    ("for SELECT DISTINCT, ORDER BY expressions must appear in select list")."""
    with pytest.raises(ValueError, match=r"can only order by projected columns"):
        _paginated(order_by=["other"], distinct=True)


def test_paginated_sql_without_distinct_still_allows_an_unprojected_order_by_column():
    """The rule is DISTINCT-only: a plain page may order by a column it doesn't project."""
    assert 'ORDER BY "other" ASC' in _paginated(order_by=["other"])


# ---------------------------------------------------------------------------
# A caller may order by a BUILT EXPRESSION rather than a column name (soda-reconciliation's
# per-side column expressions render `(<expr>) AS "<col>"` and order by `(<expr>)`). Neither
# the normalize lookup nor the DISTINCT projection check can reason about such a term by
# name, so both step aside for it instead of raising on the caller's behalf.
# ---------------------------------------------------------------------------


def test_paginated_sql_orders_by_a_built_expression_term():
    assert _paginated(order_by=[SqlExpressionStr("TRIM(code)")]) == (
        'SELECT "code",\n       "label"\nFROM "s"."t"\n' "ORDER BY (TRIM(code)) ASC\nLIMIT 10\nOFFSET 0;"
    )


def test_a_built_expression_term_is_not_looked_up_in_normalize_key_columns():
    """`term in normalize_key_columns` would raise TypeError on an unhashable AST node, and
    there is no name to look up anyway — the caller folds its own expression terms."""
    assert "LOWER(" not in _paginated(
        order_by=[SqlExpressionStr("TRIM(code)")], normalize_key_columns=frozenset({"code"})
    )


def test_distinct_accepts_a_built_expression_term_it_cannot_match_by_name():
    """The caller projects the same expression aliased; this validation only speaks names."""
    sql = _paginated(order_by=[SqlExpressionStr("TRIM(code)")], distinct=True)
    assert sql.startswith('SELECT DISTINCT "code",')
    assert "ORDER BY (TRIM(code)) ASC" in sql


def test_distinct_still_rejects_an_unprojected_order_by_COLUMN_next_to_an_expression():
    """Stepping aside for expression terms must not disable the check for the names beside
    them."""
    with pytest.raises(ValueError, match=r"can only order by projected columns"):
        _paginated(order_by=[SqlExpressionStr("TRIM(code)"), "other"], distinct=True)


# ---------------------------------------------------------------------------
# The two DISTINCT shapes are not interchangeable, so their union is rejected.
# The flat select renders against the BASE table, so a caller-built expression term
# binds there. The de-duplicating CTE's outer select renders against the CTE's OUTPUT
# columns, so the same term does NOT bind there — but a case-folded plain key only
# works there. A caller asking for both gets a named error, not broken SQL.
# ---------------------------------------------------------------------------


def test_distinct_cte_rejects_an_expression_term_beside_a_normalized_key():
    with pytest.raises(ValueError, match=r"cannot combine normalize_key_columns with"):
        _paginated(
            order_by=["code", SqlExpressionStr("(a || b)")],
            normalize_key_columns=frozenset({"code"}),
            distinct=True,
        )


def test_distinct_cte_rejects_an_expression_in_the_projection():
    """Same reason from the other side: the outer select re-renders `columns` against the CTE,
    where `(a || b)` no longer resolves — it is the CTE's aliased output column by then."""
    with pytest.raises(ValueError, match=r"cannot combine normalize_key_columns with"):
        _paginated(
            order_by=["code"],
            normalize_key_columns=frozenset({"code"}),
            distinct=True,
            columns=("code", ALIAS(SqlExpressionStr("(a || b)"), "label")),
        )


def test_distinct_expression_term_without_normalization_still_renders_flat():
    """The guard is scoped to the CTE shape — it must not take away the flat shape, which is
    the one that actually serves expression callers (reference_diff never normalizes keys)."""
    sql = _paginated(
        order_by=[SqlExpressionStr("(a || b)")],
        columns=("code", ALIAS(SqlExpressionStr("(a || b)"), "label")),
        distinct=True,
    )
    assert sql.startswith("SELECT DISTINCT")
    assert "_soda_distinct_page" not in sql
    assert "ORDER BY ((a || b)) ASC" in sql


def test_distinct_with_normalized_key_and_plain_names_is_unaffected():
    """The live composed path stays exactly as it was — the guard is a tripwire, not a gate."""
    assert "_soda_distinct_page" in _paginated(
        order_by=["code"], normalize_key_columns=frozenset({"code"}), distinct=True
    )


def test_supports_row_sampling_base_is_true():
    # The recon sampling fail-loud guard fires only when this is False; base SQL sources must
    # report True so a sampled recon on them is never flipped to NOT_EVALUATED.
    assert dialect().supports_row_sampling() is True


# ---------------------------------------------------------------------------
# returns_native_boolean_values()
#
# Whether the driver hands back a real bool for a canonically BOOLEAN column.
# Consumers of query results branch on this instead of on a data source name;
# answering False changes handling for that source only, leaving every other
# source's data untouched.
# ---------------------------------------------------------------------------


def test_returns_native_boolean_values_defaults_true():
    assert dialect().returns_native_boolean_values() is True


def test_a_dialect_can_declare_a_non_native_boolean_driver():
    # SqlDialect.__init_subclass__ requires the sqlglot dialect name.
    class IntBooleanDialect(SqlDialect, sqlglot_dialect="mysql"):
        def returns_native_boolean_values(self) -> bool:
            return False

    assert IntBooleanDialect().returns_native_boolean_values() is False


def test_it_is_a_method_not_a_property():
    # Consistent with the supports_* family, so an override following that convention cannot
    # silently shadow it with a property that is always truthy.
    assert callable(SqlDialect.returns_native_boolean_values)
    assert not isinstance(SqlDialect.__dict__["returns_native_boolean_values"], property)
