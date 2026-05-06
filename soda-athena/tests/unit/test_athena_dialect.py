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
