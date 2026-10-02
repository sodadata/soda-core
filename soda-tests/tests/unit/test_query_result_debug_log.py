from __future__ import annotations

import logging
from decimal import Decimal

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logging_constants import soda_logger


class _FakeCursor:
    description = [("id",), ("zip_code",), ("amount",), ("price",)]

    def execute(self, sql: str) -> None:
        pass

    def fetchall(self) -> list[tuple]:
        return [("941935e8", "00123", 1234567.891, Decimal("12.50"))]

    def close(self) -> None:
        pass


class _FakeConnection:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


class _FakeDataSourceConnection(DataSourceConnection):
    def _create_connection(self, connection_yaml_dict: dict) -> object:
        return _FakeConnection()

    def _execute_query_get_result_row_column_name(self, column) -> str:
        return column[0]


def test_query_result_debug_log_prints_values_as_returned(caplog):
    connection = _FakeDataSourceConnection(name="fake", connection_properties={}, connection=_FakeConnection())

    with caplog.at_level(logging.DEBUG, logger=soda_logger.name):
        connection.execute_query("SELECT 1")

    result_log = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("SQL query result"))
    row = result_log.splitlines()[-1]
    assert [cell.strip() for cell in row.strip("|").split("|")] == ["941935e8", "00123", "1234567.891", "12.50"]
