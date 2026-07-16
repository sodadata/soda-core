from datetime import date

import pytest
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT, STAR, SamplerType
from soda_databricks.common.data_sources.databricks_data_source import (
    DatabricksSqlDialect,
)


@pytest.mark.parametrize(
    "sql_ast, expected_sql",
    [
        pytest.param(
            [SELECT(STAR()), FROM("a").SAMPLE(SamplerType.PERCENTAGE, 10)],
            "SELECT *\nFROM `a` TABLESAMPLE (10 PERCENT);",
            id="tablesample_percentage(10%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("b").SAMPLE(SamplerType.PERCENTAGE, 25)],
            "SELECT *\nFROM `b` TABLESAMPLE (25 PERCENT);",
            id="tablesample_percentage(25%)",
        ),
        pytest.param(
            [SELECT(STAR()), FROM("c").SAMPLE(SamplerType.PERCENTAGE, 100)],
            "SELECT *\nFROM `c` TABLESAMPLE (100 PERCENT);",
            id="tablesample_percentage(100%)",
        ),
    ],
)
def test_tablesample(sql_ast, expected_sql):
    sql_dialect: DatabricksSqlDialect = DatabricksSqlDialect()
    assert sql_dialect.build_select_sql(sql_ast) == expected_sql


@pytest.mark.parametrize(
    "sql_ast, expected_exception_message",
    [
        pytest.param(
            [SELECT(STAR()), FROM("customers").SAMPLE(SamplerType.ABSOLUTE_LIMIT, 100)],
            "Unsupported sampler type: ABSOLUTE_LIMIT",
            id="tablesample_not_supported_absolute_limit",
        ),
    ],
)
def test_tablesample_not_supported(sql_ast, expected_exception_message):
    sql_dialect: DatabricksSqlDialect = DatabricksSqlDialect()

    with pytest.raises(ValueError) as ex:
        sql_dialect.build_select_sql(sql_ast)

    assert str(ex.value) == expected_exception_message


def test_random():
    sql_dialect: DatabricksSqlDialect = DatabricksSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == "SELECT RAND()\nFROM `a`;"


def test_literal_date_pads_year_to_four_digits():
    """Spark SQL rejects DATE literals without a 4-digit year as INVALID_TYPED_LITERAL,
    and C strftime("%Y") drops the leading zeros for years < 1000 on glibc."""
    assert DatabricksSqlDialect().literal(date(200, 12, 17)) == "DATE '0200-12-17'"


class TestTimestampReverseMapping:
    """Pin the cross-source DWH dispatch invariant for Databricks.

    Native Databricks ``timestamp`` is TZ-aware in semantics (stored as an instant,
    displayed in session TZ). The reverse map must therefore resolve ``timestamp`` to
    ``SodaDataTypeName.TIMESTAMP_TZ`` so cross-source DWH transfer dispatches the
    TZ-aware value mapper for Databricks-as-source. ``timestamp_ntz`` (the explicit
    naive variant) resolves to ``SodaDataTypeName.TIMESTAMP``. A regression that
    flipped these would silently drop tzinfo from Databricks-source rows on a
    non-UTC Spark session and store a session-local wallclock instead of a UTC
    instant in the warehouse.
    """

    @pytest.mark.parametrize(
        "data_type_name, expected_canonical",
        [
            ("timestamp", SodaDataTypeName.TIMESTAMP_TZ),
            ("timestamptz", SodaDataTypeName.TIMESTAMP_TZ),
            ("timestamp with time zone", SodaDataTypeName.TIMESTAMP_TZ),
            ("timestamp_ntz", SodaDataTypeName.TIMESTAMP),
            ("timestamp without time zone", SodaDataTypeName.TIMESTAMP),
        ],
    )
    def test_reverse_map_resolves_correctly(self, data_type_name: str, expected_canonical: SodaDataTypeName) -> None:
        dialect = DatabricksSqlDialect()
        actual = dialect.get_soda_data_type_name_by_data_source_data_type_names().get(data_type_name)
        assert actual == expected_canonical, (
            f"Databricks reverse-map for {data_type_name!r} returned {actual!r}, expected "
            f"{expected_canonical!r}. A regression here would silently dispatch the wrong "
            "DWH value mapper for Databricks-as-source."
        )


def test_max_sql_statement_length_respects_databricks_16mib_cap():
    # Databricks rejects statements over its documented 16 MiB query-text
    # cap ("Query text size exceeds limit", observed live at ~30 MB on
    # 2026-06-12); 1 MiB is reserved for chars-vs-bytes skew.
    from soda_databricks.common.data_sources.databricks_data_source import (
        DatabricksSqlDialect,
    )

    sql_dialect = DatabricksSqlDialect()
    assert sql_dialect.get_max_sql_statement_length() == 15 * 1024 * 1024
    assert sql_dialect.get_max_sql_statement_length() < 16 * 1024 * 1024


def test_drop_table_does_not_emit_cascade():
    # Databricks SQL: CASCADE is valid on DROP SCHEMA only; DROP TABLE ...
    # CASCADE fails with PARSE_SYNTAX_ERROR (observed live 2026-06-12).
    from soda_core.common.sql_ast import DROP_TABLE

    sql_dialect = DatabricksSqlDialect()
    sql = sql_dialect.build_drop_table_sql(DROP_TABLE(fully_qualified_table_name="`c`.`s`.`t`", cascade=True))
    assert "CASCADE" not in sql
