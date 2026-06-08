"""Tests for the ``column_unbounded_text`` builder + chunked-insert path
+ the post-insert data-length verification in ``DataSourceTestHelper``.

The combination is load-bearing for the Phase 7+ memory tests: those
fixtures push 100 MB-per-row payloads through a SqlServer source, which
silently truncates at ``varchar(default_length)`` if the column type
isn't explicitly unbounded. The verification step catches that class
of regression at test setup time before the memory profile gets
contaminated.

Scoped to postgres + sqlserver only — other adapters haven't been
through the Phase 9 expansion yet. Will be widened once the adapter-
specific overrides (``_string_length_sql_function``, varchar(max)
mapping) are validated on snowflake / bigquery / databricks.
"""

from __future__ import annotations

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import test_datasource
from helpers.test_table import TestTableSpecification

# These tests insert real rows and query length-aggregates over them.
# The snapshot recorder normalises queries in ways that mask MIN(length)
# diffs between record/replay, so always run against a real DB.
pytestmark = pytest.mark.no_snapshot

_SUPPORTED_DATASOURCES = {"postgres", "sqlserver"}
_IS_SQLSERVER_FAMILY = test_datasource in {"sqlserver", "fabric", "synapse"}

if test_datasource not in _SUPPORTED_DATASOURCES:
    pytest.skip(
        f"unbounded_text_column tests are scoped to {_SUPPORTED_DATASOURCES}; "
        f"skipping on {test_datasource}.",
        allow_module_level=True,
    )


def _build_spec(table_purpose: str, rows: list[tuple]) -> TestTableSpecification:
    return (
        TestTableSpecification.builder()
        .table_purpose(table_purpose)
        .column_integer("id")
        .column_unbounded_text("payload")
        .rows(rows=rows)
        .build()
    )


# ---------------------------------------------------------------------------
# 1. Happy path — column_unbounded_text round-trips with the verification
# ---------------------------------------------------------------------------


def test_unbounded_text_small_payload_round_trip(
    data_source_test_helper: DataSourceTestHelper,
):
    """Sanity check: short payloads insert via the chunked path and the
    data-length verification accepts them. If this fails, every Phase 7
    memory test will fail at setup before the workload even runs."""
    payloads = ["a" * 1024, "b" * 4096, "c" * 8192]
    spec = _build_spec(
        table_purpose="unbounded_small_roundtrip",
        rows=[(i, payload) for i, payload in enumerate(payloads)],
    )
    test_table = data_source_test_helper.ensure_test_table(spec)

    # Confirm the rows actually landed with the lengths we expect — proves
    # the verification step queried the right column on the right table.
    qcol = data_source_test_helper.data_source_impl.sql_dialect.quote_default("payload")
    length_fn = data_source_test_helper._string_length_sql_function()
    sql = (
        f"SELECT MIN({length_fn}({qcol})), MAX({length_fn}({qcol})) "
        f"FROM {test_table.qualified_name}"
    )
    result = data_source_test_helper.data_source_impl.execute_query(sql)
    actual_min, actual_max = int(result.rows[0][0]), int(result.rows[0][1])
    assert actual_min == 1024, f"expected MIN=1024, got {actual_min}"
    assert actual_max == 8192, f"expected MAX=8192, got {actual_max}"


# ---------------------------------------------------------------------------
# 2. Negative path — verification raises when actual MIN < expected MIN
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _IS_SQLSERVER_FAMILY,
    reason=(
        "The data-length verification only iterates columns whose "
        "character_maximum_length is a string sentinel like 'max' (set by "
        "column_unbounded_text on the sqlserver family). Postgres maps the "
        "same builder to TEXT — which is genuinely unbounded at the DB level "
        "so truncation can't happen — and the verification correctly skips "
        "it. There's nothing to assert on postgres."
    ),
)
def test_verify_raises_when_actual_data_is_truncated(
    data_source_test_helper: DataSourceTestHelper,
):
    """The verification's job is to detect silent truncation. Simulate
    that by inserting a deliberately-short row into a freshly-built
    table, then calling the verification with a synthetic ``TestTable``
    that claims the rows are much longer. The check must raise."""
    spec = _build_spec(
        table_purpose="unbounded_verify_truncated",
        rows=[(1, "real")],  # 4-char payload — what's actually in the DB
    )
    test_table = data_source_test_helper.ensure_test_table(spec)

    # Build a parallel TestTable Python object pointing at the SAME
    # physical table but advertising an expected MIN of 1000 chars.
    # _verify_unbounded_columns_data_length reads its expectations from
    # row_values, so re-pointing them is enough to fake the mismatch.
    from helpers.test_table import TestTable

    synthetic = TestTable(
        data_source_name=test_table.data_source_name,
        dataset_prefix=test_table.dataset_prefix,
        code_name=test_table.code_name,
        unique_name=test_table.unique_name,
        qualified_name=test_table.qualified_name,
        columns=list(test_table.columns.values()),
        row_values=[(1, "x" * 1000)],  # claim payload is 1000 chars long
    )

    with pytest.raises(AssertionError) as exc_info:
        data_source_test_helper._verify_unbounded_columns_data_length(synthetic)
    msg = str(exc_info.value)
    assert "payload" in msg
    assert "1000" in msg  # expected MIN
    assert "4" in msg     # actual MIN (the truncated value)


# ---------------------------------------------------------------------------
# 3. SqlServer-specific — trailing spaces must use DATALENGTH not LEN
# ---------------------------------------------------------------------------

_TRAILING_PAYLOAD = "abc" + (" " * 1000)  # 1003 chars total, 3 non-space


def test_trailing_spaces_payload_round_trips(
    data_source_test_helper: DataSourceTestHelper,
):
    """A payload that ends in spaces is a stress test for the
    length-function choice. SqlServer's ``LEN()`` trims trailing
    spaces and would report 3 here, breaking the verification. The
    sqlserver override returns ``DATALENGTH`` which reports the raw
    byte count (1003). On postgres ``length()`` already returns 1003
    so this test exercises both paths through the same assertion."""
    spec = _build_spec(
        table_purpose="unbounded_trailing_spaces",
        rows=[(1, _TRAILING_PAYLOAD)],
    )
    # ensure_test_table runs the chunked insert then the verification.
    # If we ever regress to LEN() on sqlserver, this raises at setup.
    data_source_test_helper.ensure_test_table(spec)


@pytest.mark.skipif(
    not _IS_SQLSERVER_FAMILY,
    reason="Exercises the SqlServer LEN-vs-DATALENGTH gotcha specifically.",
)
def test_sqlserver_len_function_would_fail_verification(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch: pytest.MonkeyPatch,
):
    """Proves the ``datalength`` override is doing real work: if we
    temporarily revert ``_string_length_sql_function`` to ``len``, the
    SAME trailing-spaces payload trips the verification. Locks the
    override against accidental removal."""
    spec = _build_spec(
        table_purpose="unbounded_trailing_spaces_len_fail",
        rows=[(1, _TRAILING_PAYLOAD)],
    )

    monkeypatch.setattr(
        type(data_source_test_helper),
        "_string_length_sql_function",
        lambda self: "len",
    )

    with pytest.raises(AssertionError) as exc_info:
        data_source_test_helper.ensure_test_table(spec)
    msg = str(exc_info.value)
    assert "payload" in msg
    # LEN trims spaces → actual MIN reported as 3, expected 1003
    assert "1003" in msg


# ---------------------------------------------------------------------------
# 4. Large payload — 100 MB through the chunked path end-to-end
# ---------------------------------------------------------------------------


def test_unbounded_text_100mb_round_trip(
    data_source_test_helper: DataSourceTestHelper,
):
    """The smoking-gun test: a single 100 MB payload via
    ``column_unbounded_text`` + ``_insert_test_table_rows_chunked`` +
    ``_verify_unbounded_columns_data_length``.

    This is the exact size class the Phase 7+ memory tests rely on
    (``scaling_at_k8s_cap[1x100M]``). If 100 MB doesn't survive the
    insert with the right length, those memory measurements are
    silently invalidated.

    Slow — allocates ~200 MB on the Python side (the row tuple plus
    the INSERT SQL string) and pushes 100 MB through the DB driver.
    Roughly 5-15 s per adapter depending on disk speed."""
    payload = "x" * (100 * 1024 * 1024)
    spec = _build_spec(
        table_purpose="unbounded_100mb_roundtrip",
        rows=[(1, payload)],
    )
    test_table = data_source_test_helper.ensure_test_table(spec)

    # Independent post-hoc length check using the dialect's length fn —
    # belt-and-suspenders to confirm we read back exactly what we wrote
    # (the verification inside ensure_test_table covers the MIN; this
    # also reads MAX, so a partial truncation wouldn't sneak through).
    qcol = data_source_test_helper.data_source_impl.sql_dialect.quote_default("payload")
    length_fn = data_source_test_helper._string_length_sql_function()
    sql = (
        f"SELECT MIN({length_fn}({qcol})), MAX({length_fn}({qcol})) "
        f"FROM {test_table.qualified_name}"
    )
    result = data_source_test_helper.data_source_impl.execute_query(sql)
    actual_min, actual_max = int(result.rows[0][0]), int(result.rows[0][1])
    expected = 100 * 1024 * 1024
    assert actual_min == expected, f"expected MIN={expected}, got {actual_min}"
    assert actual_max == expected, f"expected MAX={expected}, got {actual_max}"
