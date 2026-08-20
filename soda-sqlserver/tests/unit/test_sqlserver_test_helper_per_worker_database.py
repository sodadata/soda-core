"""Per-pytest-xdist-worker database for the SQL Server test helper.

SQL Server's system catalog is per database: workers sharing one database deadlock
each other on catalog rows (DDL vs information_schema scans, SQLSTATE 40001). The
helper therefore runs each xdist worker in its own database, and tells the snapshot
wrapper to normalize that name so recordings stay portable across workers.
"""

from __future__ import annotations

import pytest
from soda_sqlserver.test_helpers.sqlserver_data_source_test_helper import (
    SNAPSHOT_DATABASE_PLACEHOLDER,
    SqlServerDataSourceTestHelper,
    per_xdist_worker_database_name,
)


def _bare_helper() -> SqlServerDataSourceTestHelper:
    # Skip __init__: it would build a DataSourceImpl. Only the naming logic is under test.
    return object.__new__(SqlServerDataSourceTestHelper)


def test_no_xdist_worker_keeps_base_database(monkeypatch) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    monkeypatch.setenv("SQLSERVER_DATABASE", "master")

    assert per_xdist_worker_database_name("master") is None
    helper = _bare_helper()
    assert helper._create_database_name() == "master"
    assert helper._per_worker_database is None
    assert helper._snapshot_extra_replacements() == {}


def test_xdist_worker_gets_its_own_database(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    monkeypatch.setenv("SQLSERVER_DATABASE", "master")

    helper = _bare_helper()
    assert helper._create_database_name() == "master_gw3"
    assert helper._per_worker_database == "master_gw3"
    assert helper._snapshot_extra_replacements() == {SNAPSHOT_DATABASE_PLACEHOLDER: "master_gw3"}


def test_opt_out_env_var_disables_per_worker_database(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw0")
    monkeypatch.setenv("SODA_TEST_SQLSERVER_DB_PER_WORKER", "false")

    assert per_xdist_worker_database_name("master") is None
    helper = _bare_helper()
    assert helper._create_database_name() == "master"
    assert helper._per_worker_database is None


@pytest.mark.parametrize("base,worker,expected", [("master", "gw0", "master_gw0"), ("soda-ci", "gw12", "soda_ci_gw12")])
def test_per_worker_name_is_a_safe_identifier(monkeypatch, base, worker, expected) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", worker)
    monkeypatch.delenv("SODA_TEST_SQLSERVER_DB_PER_WORKER", raising=False)
    assert per_xdist_worker_database_name(base) == expected


def test_connection_yaml_targets_the_per_worker_database(monkeypatch) -> None:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw1")
    monkeypatch.setenv("SQLSERVER_DATABASE", "master")
    helper = _bare_helper()
    helper.name = "primary_datasource"
    helper.dataset_prefix = [helper._create_database_name(), "some_schema"]

    yaml_str = helper._create_data_source_yaml_str()

    assert "database: 'master_gw1'" in yaml_str
