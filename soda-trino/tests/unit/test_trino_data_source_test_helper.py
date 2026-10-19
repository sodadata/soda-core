from types import SimpleNamespace

from soda_trino.common.data_sources.trino_data_source import TrinoSqlDialect
from soda_trino.test_helpers.trino_data_source_test_helper import TrinoDataSourceTestHelper


def _helper() -> TrinoDataSourceTestHelper:
    helper = TrinoDataSourceTestHelper.__new__(TrinoDataSourceTestHelper)
    helper.data_source_impl = SimpleNamespace(sql_dialect=TrinoSqlDialect())
    return helper


def test_drop_schema_if_exists_sql_has_no_trailing_semicolon():
    assert _helper().drop_schema_if_exists_sql("ci_main_x") == 'DROP SCHEMA IF EXISTS "ci_main_x" CASCADE'


def test_drop_schema_sql_has_no_trailing_semicolon():
    assert _helper().drop_schema_sql("ci_main_x") == 'DROP SCHEMA "ci_main_x" CASCADE'
