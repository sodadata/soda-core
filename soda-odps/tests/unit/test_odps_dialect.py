from soda_odps.common.data_sources.odps_data_source import OdpsSqlDialect


class TestOdpsSqlDialect:
    """Unit tests for ODPS SQL dialect."""

    def test_quote_for_ddl_with_identifier(self):
        dialect = OdpsSqlDialect()
        result = dialect.quote_for_ddl("my_table")
        assert result == "`my_table`"

    def test_quote_for_ddl_with_empty_string(self):
        dialect = OdpsSqlDialect()
        result = dialect.quote_for_ddl("")
        assert result is None

    def test_quote_for_ddl_with_none(self):
        dialect = OdpsSqlDialect()
        result = dialect.quote_for_ddl(None)
        assert result is None

    def test_default_casify(self):
        dialect = OdpsSqlDialect()
        result = dialect.default_casify("MyTable")
        assert result == "mytable"

    def test_metadata_casify(self):
        dialect = OdpsSqlDialect()
        result = dialect.metadata_casify("MyColumn")
        assert result == "mycolumn"

    def test_quote_column(self):
        dialect = OdpsSqlDialect()
        result = dialect.quote_column("my_column")
        assert result == '"my_column"'

    def test_supports_drop_table_cascade(self):
        dialect = OdpsSqlDialect()
        assert dialect.SUPPORTS_DROP_TABLE_CASCADE is False
