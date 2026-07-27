from soda_athena.common.data_sources.athena_data_source import (
    AthenaSqlDialect,
    _collapse_athena_prefixes,
)
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT, STAR


def test_random():
    sql_dialect: AthenaSqlDialect = AthenaSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


# --- Tests for catalog names containing '/' (e.g. S3 Tables catalogs) ---


class TestAthenaCatalogWithSlash:
    """Verify that Athena handles catalog names containing '/' correctly.

    AWS S3 Tables catalogs use names like 's3tablescatalog/bucket_name'.
    DatasetIdentifier.parse() splits the DQN on '/', which over-splits the catalog
    into multiple prefix elements. The Athena dialect and data source must collapse
    these back into the original catalog name.
    """

    def test_qualified_name_regular_catalog(self):
        dialect = AthenaSqlDialect()
        result = dialect._build_qualified_quoted_dataset_name(
            dataset_name="my_table",
            dataset_prefix=["awsdatacatalog", "my_schema"],
        )
        assert result == '"awsdatacatalog"."my_schema"."my_table"'

    def test_qualified_name_catalog_with_slash(self):
        dialect = AthenaSqlDialect()
        result = dialect._build_qualified_quoted_dataset_name(
            dataset_name="my_table",
            dataset_prefix=["s3tablescatalog", "bucket_name", "my_schema"],
        )
        assert result == '"s3tablescatalog/bucket_name"."my_schema"."my_table"'

    def test_qualified_name_no_prefix(self):
        dialect = AthenaSqlDialect()
        result = dialect._build_qualified_quoted_dataset_name(
            dataset_name="my_table",
            dataset_prefix=None,
        )
        assert result == '"my_table"'

    def test_qualified_name_single_prefix(self):
        dialect = AthenaSqlDialect()
        result = dialect._build_qualified_quoted_dataset_name(
            dataset_name="my_table",
            dataset_prefix=["my_schema"],
        )
        assert result == '"my_schema"."my_table"'

    def test_select_from_with_slash_catalog(self):
        dialect = AthenaSqlDialect()
        sql = dialect.build_select_sql(
            [
                SELECT(STAR()),
                FROM("my_table", ["s3tablescatalog", "bucket_name", "my_schema"]),
            ]
        )
        assert '"s3tablescatalog/bucket_name"."my_schema"."my_table"' in sql


class TestCollapseAthenaPrefixes:
    """Test the prefix collapse helper used by AthenaDataSourceImpl and AthenaSqlDialect."""

    def test_regular_prefix(self):
        assert _collapse_athena_prefixes(["awsdatacatalog", "my_schema"]) == ("awsdatacatalog", "my_schema")

    def test_slash_catalog_prefix(self):
        assert _collapse_athena_prefixes(["s3tablescatalog", "bucket_name", "my_schema"]) == (
            "s3tablescatalog/bucket_name",
            "my_schema",
        )

    def test_empty_prefix(self):
        assert _collapse_athena_prefixes([]) == (None, None)

    def test_single_prefix(self):
        assert _collapse_athena_prefixes(["awsdatacatalog"]) == ("awsdatacatalog", None)

    def test_multiple_slashes_in_catalog(self):
        assert _collapse_athena_prefixes(["s3tablescatalog", "a", "b", "my_schema"]) == (
            "s3tablescatalog/a/b",
            "my_schema",
        )


# ---------------------------------------------------------------------------
# TIME_DELTA / ADD_INTERVAL — MM time-bucket nodes. Athena engine v3 is
# Trino-based (https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html):
# seconds date_diff divided by the float seconds-per-interval, cast to int
# (date_add's value argument must be integer-typed); date_add with lowercase
# unit names.
# ---------------------------------------------------------------------------


def test_time_delta_renders_date_diff_seconds_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = AthenaSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr('"ts"'), "days", 1)
    )
    assert sql == (
        "cast(floor(date_diff('second', From_iso8601_timestamp('2020-06-20T00:00:00'), (\"ts\")) / 86400.0) as int)"
    )


def test_time_delta_date_diff_count_2_hours():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = AthenaSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr('"ts"'), "hours", 2)
    )
    assert sql == (
        "cast(floor(date_diff('second', From_iso8601_timestamp('2020-06-20T00:00:00'), (\"ts\")) / 7200.0) as int)"
    )


def test_add_interval_renders_date_add_lowercase_unit():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = AthenaSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "date_add('day', ((soda_partition__ + 1) * 1), From_iso8601_timestamp('2020-06-20T00:00:00'))"


def test_add_interval_weeks_unit_name():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = AthenaSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "weeks", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "date_add('week', ((soda_partition__ + 1) * 1), From_iso8601_timestamp('2020-06-20T00:00:00'))"


# ---------------------------------------------------------------------------
# PERCENTILE_WITHIN_GROUP — the Trino-based Athena engine does not accept the
# base WITHIN GROUP form; approx_percentile(expr, p) is its aggregate.
# ---------------------------------------------------------------------------


def test_percentile_within_group_renders_approx_percentile():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = AthenaSqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.5))
    assert sql == 'approx_percentile("c", 0.5)'


def test_supports_percentile_within_group_is_true():
    assert AthenaSqlDialect().supports_percentile_within_group() is True


# ---------------------------------------------------------------------------
# CAST — Athena splits its SQL across two parsers: DDL is Hive (FLOAT exists,
# REAL does not) while DML/expressions are Trino (REAL exists, FLOAT does
# not). The canonical type map serves DDL, so canonical FLOAT casts render the
# Trino name 'real' via the _build_cast_sql override.
# ---------------------------------------------------------------------------


def test_cast_canonical_float_renders_trino_real():
    from soda_core.common.metadata_types import SodaDataTypeName
    from soda_core.common.sql_ast import CAST, COLUMN

    sql = AthenaSqlDialect().build_expression_sql(CAST(COLUMN("c"), SodaDataTypeName.FLOAT))
    assert sql == 'CAST("c" AS real)'


def test_cast_other_canonical_types_keep_the_map():
    from soda_core.common.metadata_types import SodaDataTypeName
    from soda_core.common.sql_ast import CAST, COLUMN

    dialect = AthenaSqlDialect()
    assert dialect.build_expression_sql(CAST(COLUMN("c"), SodaDataTypeName.DOUBLE)) == 'CAST("c" AS double)'
    assert dialect.build_expression_sql(CAST(COLUMN("c"), SodaDataTypeName.VARCHAR)) == 'CAST("c" AS varchar)'


def test_cast_raw_string_type_passes_through():
    from soda_core.common.sql_ast import CAST, COLUMN

    assert AthenaSqlDialect().build_expression_sql(CAST(COLUMN("c"), "varchar")) == 'CAST("c" AS varchar)'


def test_cast_raw_string_float_also_renders_real():
    """SodaDataTypeName is a str-enum, so a raw 'float' cast type compares
    equal to SodaDataTypeName.FLOAT and takes the override too — desirable,
    since a passed-through 'float' would be invalid Trino DML anyway."""
    from soda_core.common.sql_ast import CAST, COLUMN

    assert AthenaSqlDialect().build_expression_sql(CAST(COLUMN("c"), "float")) == 'CAST("c" AS real)'


def test_ddl_float_type_name_stays_hive_float():
    """The DDL map keeps the Hive name — CREATE EXTERNAL TABLE consumers
    resolve through it and Hive has no 'real'."""
    from soda_core.common.metadata_types import SodaDataTypeName

    assert AthenaSqlDialect().get_data_source_data_type_name_for_soda_data_type_name(SodaDataTypeName.FLOAT) == "float"


# ---------------------------------------------------------------------------
# Result timestamp parsing — Athena renders timestamp(0) values without a
# fractional part, which pyathena's default converter rejects.
# ---------------------------------------------------------------------------


def test_lenient_timestamp_converter_accepts_both_precisions():
    from datetime import datetime

    from soda_athena.common.data_sources.athena_data_source_connection import (
        LenientTypeConverter,
    )

    converter = LenientTypeConverter()
    convert = converter.mappings["timestamp"]
    assert convert("2026-07-04 00:00:00.123000") == datetime(2026, 7, 4, 0, 0, 0, 123000)
    assert convert("2026-07-04 00:00:00") == datetime(2026, 7, 4)
    assert convert(None) is None
