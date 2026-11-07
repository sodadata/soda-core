"""Rules every SQL dialect must follow.

These tests check the dialect of the data source under test, picked by `TEST_DATASOURCE` like the integration suite,
but they never connect to it. CI runs the arch suite in every data source's lane, so a new dialect is covered without
being listed anywhere.
"""

from soda_core.common.sql_dialect import SqlDialect


def test_native_type_name_keys_are_lowercase(data_source_sql_dialect: SqlDialect):
    """Callers look up a `SqlDataType.name`, which is always lowercase, so a key with an uppercase letter would
    never match and its type would silently go unmapped."""
    native_type_name_map = data_source_sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    assert [name for name in native_type_name_map if name != name.lower()] == []
