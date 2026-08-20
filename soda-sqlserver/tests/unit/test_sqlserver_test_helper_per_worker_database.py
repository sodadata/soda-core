"""Per-pytest-xdist-worker database for the SQL Server test helper.

SQL Server's system catalog is per database: workers sharing one database deadlock
each other on catalog rows (DDL vs information_schema scans, SQLSTATE 40001). The
helper therefore runs each xdist worker in its own database, and tells the snapshot
wrapper to normalize that name so recordings stay portable across workers.
"""

from __future__ import annotations

import types

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


class _FakeCursor:
    def __init__(self, executed: list[str], fail: bool = False):
        self._executed = executed
        self._fail = fail

    def execute(self, sql: str):
        if self._fail:
            raise RuntimeError("CREATE DATABASE permission denied")
        self._executed.append(sql)


class _FakeConnection:
    def __init__(self, executed: list[str], fail: bool = False):
        self.closed = False
        self._cursor = _FakeCursor(executed, fail)

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


class _FakeProperties:
    """Stands in for the pydantic connection-properties model."""

    def __init__(self, database: str, login_timeout: int = 7):
        self.database = database
        self.login_timeout = login_timeout

    def model_copy(self, update: dict):
        return _FakeProperties(update.get("database", self.database), self.login_timeout)


def _helper_with_per_worker_database(monkeypatch) -> SqlServerDataSourceTestHelper:
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw2")
    monkeypatch.setenv("SQLSERVER_DATABASE", "master")
    helper = _bare_helper()
    helper.dataset_prefix = [helper._create_database_name(), "some_schema"]
    impl = types.SimpleNamespace(
        data_source_model=types.SimpleNamespace(connection_properties=_FakeProperties("master_gw2"))
    )
    helper.data_source_impl = impl
    return helper


def _patch_bootstrap_connection(monkeypatch, executed: list[str], connections: list, fail: bool = False):
    import pyodbc
    from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
        SqlServerDataSourceConnection,
    )

    connect_calls: list[dict] = []

    def fake_connect(connection_string, **kwargs):
        connect_calls.append({"connection_string": connection_string, **kwargs})
        connection = _FakeConnection(executed, fail)
        connections.append(connection)
        return connection

    monkeypatch.setattr(pyodbc, "connect", fake_connect)
    monkeypatch.setattr(
        SqlServerDataSourceConnection,
        "build_connection_string",
        staticmethod(lambda props: f"DATABASE={props.database}"),
    )
    return connect_calls


def test_per_worker_database_is_created_before_the_primary_connection_opens(monkeypatch) -> None:
    helper = _helper_with_per_worker_database(monkeypatch)
    executed: list[str] = []
    connections: list[_FakeConnection] = []
    connect_calls = _patch_bootstrap_connection(monkeypatch, executed, connections)
    order: list[str] = []
    monkeypatch.setattr(
        SqlServerDataSourceTestHelper.__mro__[1],
        "start_test_session_open_connection",
        lambda self: order.append("primary_connection_opened"),
    )
    original_ensure = helper._ensure_database_exists
    helper._ensure_database_exists = lambda db: (order.append("database_ensured"), original_ensure(db))

    helper.start_test_session_open_connection()

    assert order == ["database_ensured", "primary_connection_opened"]
    # Bootstrap connection targets the BASE database, not the one being created, with autocommit.
    assert connect_calls == [{"connection_string": "DATABASE=master", "timeout": 7, "autocommit": True}]
    assert executed == ["IF DB_ID(N'master_gw2') IS NULL CREATE DATABASE [master_gw2]"]
    assert connections[0].closed is True


def test_bootstrap_connection_is_closed_when_database_creation_fails(monkeypatch) -> None:
    helper = _helper_with_per_worker_database(monkeypatch)
    executed: list[str] = []
    connections: list[_FakeConnection] = []
    _patch_bootstrap_connection(monkeypatch, executed, connections, fail=True)

    with pytest.raises(RuntimeError, match="permission denied"):
        helper._ensure_database_exists("master_gw2")

    assert executed == []
    assert connections[0].closed is True


def test_no_database_bootstrap_without_per_worker_database(monkeypatch) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    helper = _bare_helper()
    helper._create_database_name()
    opened: list[str] = []
    monkeypatch.setattr(
        SqlServerDataSourceTestHelper.__mro__[1], "start_test_session_open_connection", lambda self: opened.append("x")
    )
    helper._ensure_database_exists = lambda db: (_ for _ in ()).throw(AssertionError("must not be called"))

    helper.start_test_session_open_connection()

    assert opened == ["x"]
