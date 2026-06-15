"""Unit tests for the memory_container plugin's test-helper integration.

These tests cover the moving parts that don't require docker:

- ``register_fixture_bulk_inserter`` / ``activate_fixture_bulk_inserter`` /
  ``deactivate_fixture_bulk_inserter`` toggle the module-global hook used by
  ``DataSourceTestHelper._create_and_insert_test_table``.
- ``_create_and_insert_test_table`` routes through the registered hook ONLY
  when the active flag is on. Every other fixture flow keeps the long-standing
  inline-SQL path so non-memory tests are unaffected.
- The memory_container plugin's ``pytest_runtest_protocol`` exports
  ``SODA_MEMTEST_FIXED_SCHEMA=dev_memory_testing`` for the duration of a
  memory test and restores the prior value on exit (success and failure paths).

The plugin's docker-dispatch + cgroup-measurement path is covered by the
existing tests under ``soda-tests/tests/memory_container/`` — those exercise
the full roundtrip and aren't repeated here.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from helpers import data_source_test_helper as helper_module
from helpers import memory_container_plugin
from helpers.data_source_test_helper import (
    DataSourceTestHelper,
    activate_fixture_bulk_inserter,
    deactivate_fixture_bulk_inserter,
    register_fixture_bulk_inserter,
)
from helpers.memory_container_plugin import (
    FIXED_SCHEMA_ENV,
    KEEP_TABLES_ENV,
    MEMORY_TEST_SCHEMA_NAME,
    SKIP_SCHEMA_DROP_ENV,
)

# ---------------------------------------------------------------------------
# Fixture isolation
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_hook_state():
    """Snapshot + restore the module-globals so a test that leaves the hook
    armed doesn't bleed into subsequent tests in the same process."""
    prev_inserter = helper_module._fixture_bulk_inserter
    prev_active = helper_module._fixture_bulk_inserter_active
    yield
    helper_module._fixture_bulk_inserter = prev_inserter
    helper_module._fixture_bulk_inserter_active = prev_active


@pytest.fixture
def fake_helper():
    """A minimal ``DataSourceTestHelper`` stand-in. The dispatch code paths
    being tested only touch ``data_source_impl`` (passed through to the hook)
    and three methods: ``_create_test_table``, ``_spec_has_unbounded_column``,
    ``_insert_test_table_rows``, ``_insert_test_table_rows_chunked``,
    ``_verify_unbounded_columns_data_length``. We mock these so the test does
    not need a real DB / fixture infrastructure."""
    helper = MagicMock(spec=DataSourceTestHelper)
    helper.data_source_impl = MagicMock(name="data_source_impl")
    helper._create_test_table = MagicMock()
    helper._insert_test_table_rows = MagicMock()
    helper._insert_test_table_rows_chunked = MagicMock()
    helper._spec_has_unbounded_column = MagicMock(return_value=False)
    helper._verify_unbounded_columns_data_length = MagicMock()
    # Bind the real method we want to exercise to the mock instance.
    helper._create_and_insert_test_table = DataSourceTestHelper._create_and_insert_test_table.__get__(
        helper, DataSourceTestHelper
    )
    return helper


@pytest.fixture
def fake_test_table():
    return MagicMock(name="test_table")


# ---------------------------------------------------------------------------
# Bulk-insert hook: register / activate / deactivate
# ---------------------------------------------------------------------------


class TestBulkInserterRegistration:
    def test_register_stores_function(self):
        sentinel = lambda impl, tt: None  # noqa: E731
        register_fixture_bulk_inserter(sentinel)
        assert helper_module._fixture_bulk_inserter is sentinel

    def test_register_replaces_prior_registration(self):
        first = MagicMock()
        second = MagicMock()
        register_fixture_bulk_inserter(first)
        register_fixture_bulk_inserter(second)
        assert helper_module._fixture_bulk_inserter is second

    def test_active_flag_defaults_off(self):
        # Even if a hook is registered, the flag must default off so the hook
        # never fires for non-memory fixture flows.
        register_fixture_bulk_inserter(MagicMock())
        assert helper_module._fixture_bulk_inserter_active is False

    def test_activate_returns_prior_state_and_sets_active(self):
        assert helper_module._fixture_bulk_inserter_active is False
        prev = activate_fixture_bulk_inserter()
        assert prev is False
        assert helper_module._fixture_bulk_inserter_active is True

    def test_activate_reports_prior_active_when_already_on(self):
        helper_module._fixture_bulk_inserter_active = True
        prev = activate_fixture_bulk_inserter()
        assert prev is True
        assert helper_module._fixture_bulk_inserter_active is True

    def test_deactivate_restores_passed_state(self):
        helper_module._fixture_bulk_inserter_active = True
        deactivate_fixture_bulk_inserter(prev_state=False)
        assert helper_module._fixture_bulk_inserter_active is False

    def test_deactivate_can_re_enable_when_pairing_with_nested_activate(self):
        # Simulates a nested scope: outer scope active=True, inner scope
        # captures prev=True, deactivate restores to True (not False).
        helper_module._fixture_bulk_inserter_active = True
        prev = activate_fixture_bulk_inserter()
        deactivate_fixture_bulk_inserter(prev)
        assert helper_module._fixture_bulk_inserter_active is True


# ---------------------------------------------------------------------------
# Three-path dispatch in ``_create_and_insert_test_table``
# ---------------------------------------------------------------------------


class TestCreateAndInsertDispatch:
    def test_hook_called_when_registered_and_active(self, fake_helper, fake_test_table):
        hook = MagicMock(name="bulk_inserter")
        register_fixture_bulk_inserter(hook)
        activate_fixture_bulk_inserter()

        fake_helper._create_and_insert_test_table(fake_test_table)

        fake_helper._create_test_table.assert_called_once_with(fake_test_table)
        hook.assert_called_once_with(fake_helper.data_source_impl, fake_test_table)
        fake_helper._insert_test_table_rows.assert_not_called()
        fake_helper._insert_test_table_rows_chunked.assert_not_called()

    def test_hook_skipped_when_registered_but_inactive(self, fake_helper, fake_test_table):
        # The critical invariant for the user-facing requirement: registering
        # the hook from conftest must NOT change behaviour for non-memory
        # tests. Only the memory_container plugin's activate call turns it on.
        hook = MagicMock(name="bulk_inserter")
        register_fixture_bulk_inserter(hook)
        # No activate_fixture_bulk_inserter() call.

        fake_helper._create_and_insert_test_table(fake_test_table)

        hook.assert_not_called()
        fake_helper._insert_test_table_rows.assert_called_once_with(fake_test_table)

    def test_no_hook_registered_falls_back_to_inline(self, fake_helper, fake_test_table):
        assert helper_module._fixture_bulk_inserter is None
        activate_fixture_bulk_inserter()  # active but nothing to call

        fake_helper._create_and_insert_test_table(fake_test_table)

        fake_helper._insert_test_table_rows.assert_called_once_with(fake_test_table)

    def test_unbounded_spec_uses_chunked_when_hook_inactive(self, fake_helper, fake_test_table):
        fake_helper._spec_has_unbounded_column.return_value = True

        fake_helper._create_and_insert_test_table(fake_test_table)

        fake_helper._insert_test_table_rows_chunked.assert_called_once_with(fake_test_table)
        fake_helper._insert_test_table_rows.assert_not_called()
        fake_helper._verify_unbounded_columns_data_length.assert_called_once_with(fake_test_table)

    def test_unbounded_spec_still_yields_to_active_hook(self, fake_helper, fake_test_table):
        # Bulk path wins over chunked when the hook is active — the adapter
        # native path handles unbounded payloads natively.
        fake_helper._spec_has_unbounded_column.return_value = True
        hook = MagicMock()
        register_fixture_bulk_inserter(hook)
        activate_fixture_bulk_inserter()

        fake_helper._create_and_insert_test_table(fake_test_table)

        hook.assert_called_once_with(fake_helper.data_source_impl, fake_test_table)
        fake_helper._insert_test_table_rows_chunked.assert_not_called()
        # Length verification still runs because the spec declared unbounded
        # columns — that invariant is independent of the insert path.
        fake_helper._verify_unbounded_columns_data_length.assert_called_once_with(fake_test_table)


# ---------------------------------------------------------------------------
# pytest_runtest_protocol — FIXED_SCHEMA_ENV management
# ---------------------------------------------------------------------------


class TestMemoryTestSchemaSelection:
    def test_memory_test_schema_name_is_dev_memory_testing(self):
        # Hard-locked: the shared schema name is the contract every memory
        # test depends on. Changing it would scatter tables across multiple
        # schemas and break the shared-cleanup assumption.
        assert MEMORY_TEST_SCHEMA_NAME == "dev_memory_testing"

    def _run_protocol(self, marker_present: bool, body_raises: bool = False):
        """Drive ``pytest_runtest_protocol`` with a fake item / no real
        dispatch. We mock the heavy parts (docker dispatch, setup_outside)
        and only assert what the protocol does to ``os.environ``.
        """
        item = MagicMock()
        marker = MagicMock()
        marker.kwargs = {"limit_mb": 64}  # passes the validation guard
        item.get_closest_marker.return_value = marker if marker_present else None

        with patch.object(memory_container_plugin, "_dispatch_and_emit_reports") as dispatch:
            if body_raises:
                dispatch.side_effect = RuntimeError("simulated")
            return memory_container_plugin.pytest_runtest_protocol(item, None), dispatch

    def test_non_memory_test_does_not_touch_env(self, monkeypatch):
        # Tests without the memory_container marker must not perturb the env.
        monkeypatch.delenv(FIXED_SCHEMA_ENV, raising=False)
        monkeypatch.delenv(SKIP_SCHEMA_DROP_ENV, raising=False)
        monkeypatch.delenv(KEEP_TABLES_ENV, raising=False)
        result, dispatch = self._run_protocol(marker_present=False)
        assert result is None  # falls back to default protocol
        assert SKIP_SCHEMA_DROP_ENV not in os.environ
        assert KEEP_TABLES_ENV not in os.environ
        assert FIXED_SCHEMA_ENV not in os.environ
        dispatch.assert_not_called()

    def test_schema_env_set_during_dispatch(self, monkeypatch):
        # During the protocol's dispatch the env var must reflect
        # ``dev_memory_testing`` so the in-container helper picks it up.
        monkeypatch.delenv(FIXED_SCHEMA_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")

        observed = {}

        def capture(item, limit_mb):
            observed["env"] = os.environ.get(FIXED_SCHEMA_ENV)
            observed["skip_drop"] = os.environ.get(SKIP_SCHEMA_DROP_ENV)
            observed["keep_tables"] = os.environ.get(KEEP_TABLES_ENV)

        with patch.object(memory_container_plugin, "_dispatch_and_emit_reports", side_effect=capture):
            item = MagicMock()
            marker = MagicMock()
            marker.kwargs = {"limit_mb": 64}
            item.get_closest_marker.return_value = marker
            memory_container_plugin.pytest_runtest_protocol(item, None)

        assert observed["env"] == "dev_memory_testing"
        # SKIP_SCHEMA_DROP must also be active during dispatch so the
        # in-container helper does NOT drop dev_memory_testing — the whole
        # point of the shared schema is "create once, reuse forever".
        assert observed["skip_drop"] == "1"
        # KEEP_TABLES_ENV must also be active so any table-level DROP
        # call (per-test __finalize_outside__, obsolete-table eviction)
        # becomes a no-op for the duration of the memory test.
        assert observed["keep_tables"] == "1"

    def test_schema_env_restored_after_success(self, monkeypatch):
        monkeypatch.delenv(FIXED_SCHEMA_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        self._run_protocol(marker_present=True)
        assert FIXED_SCHEMA_ENV not in os.environ

    def test_schema_env_restored_after_failure(self, monkeypatch):
        monkeypatch.delenv(FIXED_SCHEMA_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        try:
            self._run_protocol(marker_present=True, body_raises=True)
        except RuntimeError:
            pass
        assert FIXED_SCHEMA_ENV not in os.environ

    def test_schema_env_restored_to_prior_value(self, monkeypatch):
        # If something outside the plugin had set the env var (e.g. an outer
        # CI script), the plugin must restore that prior value rather than
        # leaving ``dev_memory_testing`` behind.
        monkeypatch.setenv(FIXED_SCHEMA_ENV, "outer_value")
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        self._run_protocol(marker_present=True)
        assert os.environ[FIXED_SCHEMA_ENV] == "outer_value"

    def test_skip_drop_env_restored_after_success(self, monkeypatch):
        # The SKIP_SCHEMA_DROP_ENV must come back to its prior state so an
        # outer non-memory test in the same process doesn't accidentally
        # see the skip flag still set.
        monkeypatch.delenv(SKIP_SCHEMA_DROP_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        self._run_protocol(marker_present=True)
        assert SKIP_SCHEMA_DROP_ENV not in os.environ

    def test_skip_drop_env_restored_after_failure(self, monkeypatch):
        monkeypatch.delenv(SKIP_SCHEMA_DROP_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        try:
            self._run_protocol(marker_present=True, body_raises=True)
        except RuntimeError:
            pass
        assert SKIP_SCHEMA_DROP_ENV not in os.environ

    def test_keep_tables_env_restored_after_success(self, monkeypatch):
        monkeypatch.delenv(KEEP_TABLES_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        self._run_protocol(marker_present=True)
        assert KEEP_TABLES_ENV not in os.environ

    def test_keep_tables_env_restored_after_failure(self, monkeypatch):
        monkeypatch.delenv(KEEP_TABLES_ENV, raising=False)
        monkeypatch.setenv("SODA_MEMORY_TESTS", "true")
        try:
            self._run_protocol(marker_present=True, body_raises=True)
        except RuntimeError:
            pass
        assert KEEP_TABLES_ENV not in os.environ


class TestDropTestTableGate:
    """When ``SODA_MEMTEST_KEEP_TABLES=1`` is set, ``_drop_test_table`` must
    be a no-op so the dev_memory_testing fixtures persist across runs.
    Without the flag set, the historical drop SQL must still execute exactly
    as before — non-memory tests depend on it."""

    def _make_helper(self):
        helper = MagicMock(spec=DataSourceTestHelper)
        helper.data_source_impl = MagicMock()
        helper._cascade_drop_table = MagicMock(return_value=True)
        helper.dataset_prefix = ["soda", "dev"]
        helper._drop_test_table = DataSourceTestHelper._drop_test_table.__get__(helper, DataSourceTestHelper)
        return helper

    def test_drop_skipped_when_keep_tables_set(self, monkeypatch):
        monkeypatch.setenv(KEEP_TABLES_ENV, "1")
        helper = self._make_helper()
        helper._drop_test_table("SODATEST_x")
        helper.data_source_impl.execute_update.assert_not_called()

    def test_drop_runs_when_keep_tables_unset(self, monkeypatch):
        monkeypatch.delenv(KEEP_TABLES_ENV, raising=False)
        helper = self._make_helper()
        helper._drop_test_table("SODATEST_x")
        helper.data_source_impl.execute_update.assert_called_once()

    def test_drop_skipped_for_truthy_variants(self, monkeypatch):
        helper = self._make_helper()
        for value in ("1", "true", "TRUE", "yes", "on"):
            monkeypatch.setenv(KEEP_TABLES_ENV, value)
            helper.data_source_impl.execute_update.reset_mock()
            helper._drop_test_table("SODATEST_x")
            helper.data_source_impl.execute_update.assert_not_called(), value
