"""Rigorous end-to-end verification of the memory_container ``setup_outside``
architecture (Phase 6 v2 / ``__prepare_outside__`` / ``__finalize_outside__``).

The framework is supposed to:
  1. Create a host-side ``DataSourceTestHelper`` matching the in-test fixture
     (same ``name``, same ``start_test_session()``/``end_test_session()``)
  2. Call ``__prepare_outside__(host_helper, **parametrize_kwargs)`` BEFORE
     docker dispatch — so heavy setup work doesn't count toward the capped peak
  3. Propagate the helper's schema name to the container via
     ``SODA_MEMTEST_FIXED_SCHEMA`` so ``ensure_test_table`` dedups against
     the host-created table (no recreation cost in the capped container)
  4. Call ``__finalize_outside__(host_helper, **parametrize_kwargs)`` AFTER
     the container exits — so cleanup is also outside the cap

This module asserts that DB state is exactly what we expect at every step:
  - **Before** any prepare: the verification table doesn't exist
  - **Inside the container**: the table exists with the exact rows from
    ``__prepare_outside__``, schema matches the host's natural schema name
  - **After** finalize: the table no longer exists

Tests are ordered alphabetically by name (``test_aaa…`` < ``test_bbb…`` <
``test_zzz…``) so the precondition → main → postcondition sequence is
respected without external plugins.
"""

from __future__ import annotations

import os

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

_TABLE_PURPOSE = "memtest_setup_outside_verification"
_EXPECTED_ROWS = [(1, "alpha"), (2, "beta"), (3, "gamma")]


def _verification_spec() -> TestTableSpecification:
    return (
        TestTableSpecification.builder()
        .table_purpose(_TABLE_PURPOSE)
        .column_integer("id")
        .column_varchar("label")
        .rows(rows=_EXPECTED_ROWS)
        .build()
    )


def _table_unique_name(dsh: DataSourceTestHelper) -> str:
    return dsh._create_test_table_python_object(_verification_spec()).unique_name


def _table_qualified_name(dsh: DataSourceTestHelper) -> str:
    """Fully-qualified ``"schema"."table"`` ready to splice into SQL."""
    table = dsh._create_test_table_python_object(_verification_spec())
    return table.qualified_name


def _table_exists(dsh: DataSourceTestHelper) -> bool:
    schema = dsh._base_schema_name
    table_lower = _table_unique_name(dsh).lower()
    sql = (
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND lower(table_name) = '{table_lower}')"
    )
    return bool(dsh.data_source_impl.execute_query(sql).rows[0][0])


def _table_row_count(dsh: DataSourceTestHelper) -> int:
    sql = f"SELECT COUNT(*) FROM {_table_qualified_name(dsh)}"
    return int(dsh.data_source_impl.execute_query(sql).rows[0][0])


def _table_rows_set(dsh: DataSourceTestHelper) -> set:
    sql = f"SELECT id, label FROM {_table_qualified_name(dsh)} ORDER BY id"
    rows = dsh.data_source_impl.execute_query(sql).rows
    return {tuple(r) for r in rows}


# ---------------------------------------------------------------------------
# Module-level hooks discovered by the memory_container plugin.
# ---------------------------------------------------------------------------


def __prepare_outside__(data_source_test_helper: DataSourceTestHelper) -> None:
    """Plugin invokes this on the HOST before docker dispatch. Create the
    verification table and assert it was actually created (catches any silent
    failure in ``ensure_test_table``)."""
    test_table = data_source_test_helper.ensure_test_table(_verification_spec())

    # Sanity: the helper claims it created the table.
    assert test_table is not None, "__prepare_outside__: ensure_test_table returned None"

    # Verify directly against postgres that the table is materialised with
    # the rows we asked for. The plugin's promise is that, when the container
    # runs ensure_test_table with this same spec, it finds the table and
    # skips recreation — that only works if the table actually exists and
    # has the right row count.
    assert _table_exists(data_source_test_helper), (
        f"__prepare_outside__: table {test_table.unique_name} was not "
        f"materialised in schema {data_source_test_helper._base_schema_name}"
    )
    actual_count = _table_row_count(data_source_test_helper)
    assert actual_count == len(_EXPECTED_ROWS), (
        f"__prepare_outside__: expected {len(_EXPECTED_ROWS)} rows in "
        f"{test_table.qualified_name}, got {actual_count}"
    )


def __finalize_outside__(data_source_test_helper: DataSourceTestHelper) -> None:
    """Plugin invokes this on the HOST after docker dispatch. Drop the table
    and assert it was actually dropped."""
    table_name = _table_unique_name(data_source_test_helper)
    data_source_test_helper._drop_test_table(table_name=table_name)
    assert not _table_exists(data_source_test_helper), (
        f"__finalize_outside__: table {table_name} still exists in schema "
        f"{data_source_test_helper._base_schema_name} after _drop_test_table"
    )


# ---------------------------------------------------------------------------
# Three ordered tests: precondition, main (in container), postcondition.
# Alphabetical sort order (aaa < bbb < zzz) ensures the sequence holds even
# without pytest-order.
# ---------------------------------------------------------------------------


def test_aaa_precondition_table_absent(data_source_test_helper: DataSourceTestHelper) -> None:
    """Before the memory_container test runs, the verification table must NOT
    exist. If a previous run left debris, drop it now so the main test starts
    from a clean slate."""
    table_name = _table_unique_name(data_source_test_helper)
    # Defensive: drop leftover from a previously-crashed run, if any.
    if _table_exists(data_source_test_helper):
        data_source_test_helper._drop_test_table(table_name=table_name)
    assert not _table_exists(data_source_test_helper), (
        f"Precondition failed: table {table_name} still exists after defensive drop. "
        "Investigate manually before re-running this suite."
    )


@pytest.mark.memory_container(limit_mb=256, setup_outside=True)
def test_bbb_container_sees_host_created_table(data_source_test_helper: DataSourceTestHelper) -> None:
    """Runs INSIDE the memory-capped container. By the time this body executes,
    ``__prepare_outside__`` should have:
      - Created the verification table on the host
      - Populated it with the expected rows
      - Caused the plugin to set ``SODA_MEMTEST_FIXED_SCHEMA`` to the host
        helper's schema name

    So the container's ``data_source_test_helper`` (same fixture as a normal
    test would use) should:
      - Compute the SAME schema name (via ``_create_schema_name`` reading
        the env var)
      - Find the existing table via ``ensure_test_table`` dedup
      - NOT recreate the table (no expensive INSERT INTO ... VALUES rebuild)
      - See the exact rows the host inserted
    """
    # The schema in the container should match the host's natural schema name
    # — propagated via SODA_MEMTEST_FIXED_SCHEMA. The plugin sets that env;
    # the helper's _create_schema_name short-circuits on it.
    fixed_schema_env = os.environ.get("SODA_MEMTEST_FIXED_SCHEMA")
    assert fixed_schema_env, (
        "SODA_MEMTEST_FIXED_SCHEMA should be set inside the container by the "
        "memory_container plugin when setup_outside=True"
    )
    assert data_source_test_helper._base_schema_name.lower() == fixed_schema_env.lower(), (
        f"Container helper schema ({data_source_test_helper._base_schema_name}) "
        f"does not match SODA_MEMTEST_FIXED_SCHEMA ({fixed_schema_env}). "
        "The helper's _create_schema_name override is not honouring the env var."
    )

    # CI-mode protection: the plugin must also set SODA_MEMTEST_SKIP_SCHEMA_DROP=1
    # so that when GITHUB_ACTIONS is set (CI), the in-container helper's
    # start_test_session_ensure_schema doesn't drop the schema the host
    # populated via __prepare_outside__. Without this, the entire setup_outside
    # win silently regresses in CI.
    skip_drop = os.environ.get("SODA_MEMTEST_SKIP_SCHEMA_DROP", "").lower()
    assert skip_drop in ("1", "true", "yes", "on"), (
        f"SODA_MEMTEST_SKIP_SCHEMA_DROP must be truthy inside the container "
        f"when setup_outside=True (got {skip_drop!r}). Without this, CI runs "
        f"(GITHUB_ACTIONS=true) silently lose the Phase 6 v2 optimization."
    )

    # Calling ensure_test_table should dedup against the host-created table.
    test_table = data_source_test_helper.ensure_test_table(_verification_spec())
    assert _table_exists(data_source_test_helper), (
        f"Container expected to see existing table {test_table.unique_name} in "
        f"schema {data_source_test_helper._base_schema_name} (created by "
        f"__prepare_outside__) — but information_schema says it's missing."
    )

    # And the rows should be exactly what __prepare_outside__ inserted.
    actual_rows = _table_rows_set(data_source_test_helper)
    expected_rows = {tuple(r) for r in _EXPECTED_ROWS}
    assert actual_rows == expected_rows, (
        f"Container sees different rows than __prepare_outside__ inserted.\n"
        f"  expected: {expected_rows}\n"
        f"  actual:   {actual_rows}"
    )


def test_zzz_postcondition_table_dropped(data_source_test_helper: DataSourceTestHelper) -> None:
    """After the memory_container test runs, ``__finalize_outside__`` should
    have dropped the verification table. This test asserts that — if the
    framework's finalize hook didn't fire (or failed silently), this test
    catches it."""
    table_name = _table_unique_name(data_source_test_helper)
    if _table_exists(data_source_test_helper):
        # Clean up the leak (so subsequent runs of this suite start clean)
        # before failing the test.
        try:
            data_source_test_helper._drop_test_table(table_name=table_name)
        finally:
            pytest.fail(
                f"Postcondition failed: table {table_name} still exists in "
                f"schema {data_source_test_helper._base_schema_name} after "
                f"__finalize_outside__ should have dropped it. The memory_container "
                f"plugin's finalize hook is not firing correctly (or "
                f"__finalize_outside__'s drop silently failed)."
            )
