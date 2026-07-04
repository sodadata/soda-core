import pytest
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT, STAR, SamplerType
from soda_snowflake.common.data_sources.snowflake_data_source import SnowflakeSqlDialect


@pytest.mark.parametrize(
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(STAR()), FROM("a").SAMPLE(SamplerType.PERCENTAGE, 10)],
            'SELECT *\nFROM "a" TABLESAMPLE (10);',
            id="tablesample_percentage(10%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("b").SAMPLE(SamplerType.PERCENTAGE, 25)],
            'SELECT *\nFROM "b" TABLESAMPLE (25);',
            id="tablesample_percentage(25%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("c").SAMPLE(SamplerType.PERCENTAGE, 100)],
            'SELECT *\nFROM "c" TABLESAMPLE (100);',
            id="tablesample_percentage(100%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("d").SAMPLE(SamplerType.ABSOLUTE_LIMIT, 50)],
            'SELECT *\nFROM "d" TABLESAMPLE (50 ROWS);',
            id="tablesample_absolute_limit(50)",
        ),
    ],
)
def test_tablesample(sql_ast, expected_sql):
    sql_dialect: SnowflakeSqlDialect = SnowflakeSqlDialect()
    assert sql_dialect.build_select_sql(sql_ast) == expected_sql


def test_random():
    sql_dialect: SnowflakeSqlDialect = SnowflakeSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())\nFROM "a";'


# ---------------------------------------------------------------------------
# Unbounded VARCHAR / TEXT normalisation — Snowflake reports any unbounded
# text column with CHARACTER_MAXIMUM_LENGTH=16777216 (its native max) in
# INFORMATION_SCHEMA.COLUMNS. Consumers of column metadata (cross-source
# CREATE TABLE on other dialects in particular) can't render a literal
# VARCHAR(16777216), so the dialect normalises that to canonical TEXT with
# no length.
# ---------------------------------------------------------------------------

# Layout matches Snowflake's INFORMATION_SCHEMA.COLUMNS query in
# build_column_metadatas_from_query_result. Column names are uppercase
# because Snowflake's default_casify uppercases identifiers.
_COLUMN_LAYOUT = (
    ("COLUMN_NAME",),
    ("DATA_TYPE",),
    ("CHARACTER_MAXIMUM_LENGTH",),
    ("NUMERIC_PRECISION",),
    ("NUMERIC_SCALE",),
    ("DATETIME_PRECISION",),
)


def _build_metadatas(rows):
    return SnowflakeSqlDialect().build_column_metadatas_from_query_result(
        QueryResult(rows=rows, columns=_COLUMN_LAYOUT)
    )


def test_unbounded_varchar_normalised_to_text_without_length():
    """An unbounded VARCHAR column (CHARACTER_MAXIMUM_LENGTH=16777216) must
    be rewritten to canonical TEXT with no length, so cross-source CREATE
    TABLE on dialects that can't represent VARCHAR(16777216) still works."""
    metadatas = _build_metadatas([("note", "varchar", 16777216, None, None, None)])
    assert metadatas[0].sql_data_type.name == "text"
    assert metadatas[0].sql_data_type.character_maximum_length is None


def test_unbounded_text_normalised_to_text_without_length():
    metadatas = _build_metadatas([("note", "text", 16777216, None, None, None)])
    assert metadatas[0].sql_data_type.name == "text"
    assert metadatas[0].sql_data_type.character_maximum_length is None


def test_bounded_varchar_passes_through_unchanged():
    """A VARCHAR with an explicit length below the unbounded sentinel must
    not be rewritten — only the sentinel value is normalised."""
    metadatas = _build_metadatas([("short", "varchar", 50, None, None, None)])
    assert metadatas[0].sql_data_type.name == "varchar"
    assert metadatas[0].sql_data_type.character_maximum_length == 50


def test_non_text_types_unaffected():
    """A numeric column carrying CHARACTER_MAXIMUM_LENGTH=None must not be
    touched by the normalisation (defensive: the strip pass key on type)."""
    metadatas = _build_metadatas([("amount", "numeric", None, 38, 2, None)])
    assert metadatas[0].sql_data_type.name == "numeric"
    assert metadatas[0].sql_data_type.numeric_precision == 38
    assert metadatas[0].sql_data_type.numeric_scale == 2


def test_reverse_map_defensive_string_aliases():
    from soda_core.common.metadata_types import SodaDataTypeName

    dialect = SnowflakeSqlDialect()
    reverse_map = dialect.get_soda_data_type_name_by_data_source_data_type_names()
    assert reverse_map["nchar"] == SodaDataTypeName.CHAR
    assert reverse_map["nvarchar"] == SodaDataTypeName.VARCHAR
    assert reverse_map["nvarchar2"] == SodaDataTypeName.VARCHAR
    assert reverse_map["char varying"] == SodaDataTypeName.VARCHAR
    assert reverse_map["nchar varying"] == SodaDataTypeName.VARCHAR


def test_get_large_numeric_cast_type_name_is_float():
    assert SnowflakeSqlDialect().get_large_numeric_cast_type_name() == "FLOAT"
