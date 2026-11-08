"""Rules every SQL dialect must follow.

The repository running these tests parametrizes `arch_sql_dialect`, so the same rules check that
repository's dialects. The tests never connect, so they need no connection settings.
"""

from soda_core.common.sql_dialect import SqlDialect


def test_native_type_name_keys_are_lowercase(arch_sql_dialect: SqlDialect):
    """Callers look up a `SqlDataType.name`, which is always lowercase, so a key with an uppercase letter would
    never match and its type would silently go unmapped."""
    native_type_name_map = arch_sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()

    assert [name for name in native_type_name_map if name != name.lower()] == []
