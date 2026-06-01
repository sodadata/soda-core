from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameSqlDialect,
)


def test_random():
    sql_dialect: SparkDataFrameSqlDialect = SparkDataFrameSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"


def test_legacy_mode_prefix_indexes_unchanged():
    dialect = SparkDataFrameSqlDialect()
    assert dialect.use_catalog is False
    assert dialect.get_database_prefix_index() is None
    assert dialect.get_schema_prefix_index() == 0


def test_catalog_mode_prefix_indexes():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    assert dialect.use_catalog is True
    assert dialect.get_database_prefix_index() == 0
    assert dialect.get_schema_prefix_index() == 1


def test_legacy_create_schema_sql_uses_single_prefix():
    dialect = SparkDataFrameSqlDialect()
    sql = dialect.create_schema_if_not_exists_sql(["my_schema"])
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_schema`;"


def test_catalog_mode_create_schema_sql_qualifies_with_catalog():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    sql = dialect.create_schema_if_not_exists_sql(["my_cat", "my_schema"])
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_schema`;"


def test_catalog_mode_create_schema_sql_no_semicolon_option():
    dialect = SparkDataFrameSqlDialect(use_catalog=True)
    sql = dialect.create_schema_if_not_exists_sql(["my_cat", "my_schema"], add_semicolon=False)
    assert sql == "CREATE SCHEMA IF NOT EXISTS `my_cat`.`my_schema`"
