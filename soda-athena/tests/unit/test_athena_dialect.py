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
# TIME_DELTA / ADD_INTERVAL — MM time-bucket nodes (OBSL-1036). Athena engine
# v3 is Trino-based (https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html):
# seconds date_diff divided by the float seconds-per-interval (v3
# athena_data_source.py:269-273) with the v3-trino int cast added (date_add's
# value argument must be integer-typed); date_add with lowercase unit names.
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


def test_add_interval_renders_date_add_lowercase_unit():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = AthenaSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "date_add('day', ((soda_partition__ + 1) * 1), From_iso8601_timestamp('2020-06-20T00:00:00'))"


# ---------------------------------------------------------------------------
# PERCENTILE_WITHIN_GROUP — the base WITHIN GROUP form is not accepted by the
# Trino-based Athena engine; v3 rendered approx_percentile(expr, p)
# (athena_data_source.py:256-259).
# ---------------------------------------------------------------------------


def test_percentile_within_group_renders_approx_percentile():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = AthenaSqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.5))
    assert sql == 'approx_percentile("c", 0.5)'


def test_supports_percentile_within_group_is_true():
    assert AthenaSqlDialect().supports_percentile_within_group() is True
