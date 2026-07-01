"""Unit tests for the fixture self-healing added to DataSourceTestHelper.

Covers the 2026-06-11 hardening:
  * verify_test_table_row_count treats an unparseable count as INVALID
    (previously ValueError -> "valid", which let an empty/partially
    populated fixture silently feed every subsequent run).
  * _drop_test_table honors force=True even under SODA_MEMTEST_KEEP_TABLES
    (previously the recreate path's drop was a silent no-op, the CREATE hit
    DuplicateTable, and a broken fixture stayed wedged forever).
  * Inside a memory container, ensure_test_table refuses to "recreate" a
    host-prepared fixture from its stub spec (that would replace the real
    payload with NULL placeholder rows).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper


def _make_helper(count_result) -> DataSourceTestHelper:
    helper = DataSourceTestHelper.__new__(DataSourceTestHelper)
    helper.dataset_prefix = ["db", "schema"]
    helper.data_source_impl = MagicMock()
    helper.data_source_impl.sql_dialect.qualify_dataset_name.return_value = '"db"."schema"."t"'
    helper.data_source_impl.execute_query.return_value = SimpleNamespace(rows=count_result)
    return helper


def _spec(n_rows: int, unique_name: str = "SODATEST_fixture_x_abc123") -> MagicMock:
    spec = MagicMock()
    spec.unique_name = unique_name
    spec.row_values = [("v",)] * n_rows
    spec.name = "fixture_x"
    return spec


class TestVerifyTestTableRowCount:
    def test_matching_count_is_valid(self):
        assert _make_helper([[5]]).verify_test_table_row_count(_spec(5)) is True

    def test_mismatched_count_is_invalid(self):
        # The empty-fixture case: table exists but holds 0 of 5 rows.
        assert _make_helper([[0]]).verify_test_table_row_count(_spec(5)) is False

    @pytest.mark.parametrize(
        "rows",
        [[["not_a_number"]], [[None]], []],
        ids=["unparseable", "null", "no_rows"],
    )
    def test_unverifiable_count_is_invalid(self, rows):
        # Previously ValueError -> "valid"; TypeError/IndexError propagated.
        # All three must now mean "recreate the table".
        assert _make_helper(rows).verify_test_table_row_count(_spec(5)) is False


class TestDropTestTableForce:
    def _make_helper(self) -> DataSourceTestHelper:
        helper = DataSourceTestHelper.__new__(DataSourceTestHelper)
        helper.dataset_prefix = ["db", "schema"]
        helper.data_source_impl = MagicMock()
        helper.data_source_impl.sql_dialect.qualify_dataset_name.return_value = '"db"."schema"."t"'
        helper.data_source_impl.sql_dialect.build_drop_table_sql.return_value = "DROP TABLE t"
        return helper

    def test_keep_tables_skips_convenience_drop(self, monkeypatch):
        monkeypatch.setenv("SODA_MEMTEST_KEEP_TABLES", "1")
        helper = self._make_helper()

        helper._drop_test_table("t")

        helper.data_source_impl.execute_update.assert_not_called()

    def test_force_drop_overrides_keep_tables(self, monkeypatch):
        # Correctness drops (failed row-count verification) must actually
        # drop, or the subsequent CREATE hits DuplicateTable forever.
        monkeypatch.setenv("SODA_MEMTEST_KEEP_TABLES", "1")
        helper = self._make_helper()

        helper._drop_test_table("t", force=True)

        helper.data_source_impl.execute_update.assert_called_once_with("DROP TABLE t")


class TestInContainerStubGuard:
    def test_refuses_to_recreate_host_prepared_table_from_stub(self, monkeypatch):
        unique_name = "SODATEST_fixture_x_abc123"
        monkeypatch.setenv("SODA_IN_MEMORY_CONTAINER", "1")
        monkeypatch.setenv("SODA_MEMTEST_FORCED_TABLE_NAMES", f'["{unique_name}"]')

        helper = DataSourceTestHelper.__new__(DataSourceTestHelper)
        helper.dataset_prefix = ["db", "schema"]
        helper.data_source_impl = MagicMock()
        helper.data_source_impl.sql_dialect.qualify_dataset_name.return_value = '"db"."schema"."t"'
        helper.test_tables = {}
        helper._ensured_test_tables = {}
        helper.existing_test_table_names = [unique_name]
        # Table exists but fails verification (e.g. interrupted population).
        helper.verify_test_table_row_count = lambda spec: False

        spec = _spec(5, unique_name=unique_name)
        spec.columns = []
        spec.table_type = MagicMock()

        with pytest.raises(AssertionError, match="Refusing to recreate"):
            helper.ensure_test_table(spec)

        helper.data_source_impl.execute_update.assert_not_called()  # nothing dropped or created
