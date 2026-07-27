from __future__ import annotations

import random
import time

import pyodbc
import pytest
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
)


def deadlock_error() -> pyodbc.Error:
    return pyodbc.Error(
        "40001",
        "[40001] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
        "Transaction (Process ID 71) was deadlocked on lock resources with "
        "another process and has been chosen as the deadlock victim. "
        "Rerun the transaction. (1205) (SQLExecDirectW)",
    )


class FakeCursor:
    def __init__(self, connection: FakeConnection):
        self._connection = connection
        self.description = [("col",)]
        self.rowcount = 1

    def execute(self, sql: str) -> None:
        self._connection.executed_sqls.append(sql)
        if self._connection.outcomes:
            outcome = self._connection.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome

    def fetchall(self) -> list[tuple]:
        return [("row1",)]

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self, outcomes: list):
        # One entry per expected execute() call: an Exception to raise, or None for success.
        self.outcomes = outcomes
        self.executed_sqls: list[str] = []
        self.rollback_count = 0
        self.commit_count = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rollback_count += 1

    def commit(self) -> None:
        self.commit_count += 1


def make_data_source_connection(fake_connection: FakeConnection) -> SqlServerDataSourceConnection:
    data_source_connection = SqlServerDataSourceConnection.__new__(SqlServerDataSourceConnection)
    data_source_connection.name = "test_sqlserver"
    data_source_connection.connection = fake_connection
    return data_source_connection


def test_execute_query_reraises_when_deadlock_persists(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    fake_connection = FakeConnection(outcomes=[deadlock_error(), deadlock_error(), deadlock_error()])
    data_source_connection = make_data_source_connection(fake_connection)

    with pytest.raises(pyodbc.Error):
        data_source_connection.execute_query("SELECT 1")

    assert len(fake_connection.executed_sqls) == SqlServerDataSourceConnection.DEADLOCK_MAX_ATTEMPTS
    assert fake_connection.rollback_count == SqlServerDataSourceConnection.DEADLOCK_MAX_ATTEMPTS - 1


def test_deadlock_backoff_is_exponential_with_jitter(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda seconds: sleeps.append(seconds))
    # Make the jitter deterministic: uniform(0, cap) -> cap, so the recorded
    # sleeps expose the exponential caps the jitter is drawn from.
    monkeypatch.setattr(random, "uniform", lambda low, high: high)
    fake_connection = FakeConnection(outcomes=[deadlock_error(), deadlock_error(), deadlock_error()])
    data_source_connection = make_data_source_connection(fake_connection)

    with pytest.raises(pyodbc.Error):
        data_source_connection.execute_query("SELECT 1")

    base = SqlServerDataSourceConnection.DEADLOCK_RETRY_BACKOFF_SECONDS
    assert base < 0.5
    assert sleeps == [base, base * 2]


def test_execute_update_retries_after_deadlock(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    fake_connection = FakeConnection(outcomes=[deadlock_error(), None])
    data_source_connection = make_data_source_connection(fake_connection)

    rowcount = data_source_connection.execute_update("DROP SCHEMA [test_schema];")

    assert rowcount == 1
    assert len(fake_connection.executed_sqls) == 2
    assert fake_connection.rollback_count == 1
    assert fake_connection.commit_count == 1


def test_execute_query_does_not_retry_non_deadlock_errors(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    syntax_error = pyodbc.ProgrammingError("42000", "[42000] Incorrect syntax near 'SELEC'.")
    fake_connection = FakeConnection(outcomes=[syntax_error, None])
    data_source_connection = make_data_source_connection(fake_connection)

    with pytest.raises(pyodbc.ProgrammingError):
        data_source_connection.execute_query("SELEC 1")

    assert len(fake_connection.executed_sqls) == 1
    assert fake_connection.rollback_count == 0


def test_execute_query_retries_after_deadlock(monkeypatch) -> None:
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    fake_connection = FakeConnection(outcomes=[deadlock_error(), None])
    data_source_connection = make_data_source_connection(fake_connection)

    query_result = data_source_connection.execute_query("SELECT 1")

    assert query_result.rows == [("row1",)]
    assert len(fake_connection.executed_sqls) == 2
    assert fake_connection.rollback_count == 1
