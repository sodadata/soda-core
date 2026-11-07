"""Rules every SQL dialect must follow.

These tests check the dialects of every registered data source in one run, whatever `TEST_DATASOURCE` is. They never
connect, so they need no connection settings. A data source is covered as soon as it is registered with
`DataSourceTestHelper.register`.
"""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.sql_dialect import SqlDialect

registered_sql_dialects = [
    pytest.param(sql_dialect, id=label) for label, sql_dialect in DataSourceTestHelper.create_registered_sql_dialects()
]


@pytest.mark.parametrize("sql_dialect", registered_sql_dialects)
def test_native_type_name_keys_are_lowercase(sql_dialect: SqlDialect):
    """Callers look up a `SqlDataType.name`, which is always lowercase, so a key with an uppercase letter would
    never match and its type would silently go unmapped."""
    native_type_name_map = sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    assert [name for name in native_type_name_map if name != name.lower()] == []
