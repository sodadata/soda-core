import logging

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import COUNT, DISTINCT, ORDINAL_POSITION, TUPLE
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_oracle.common.data_sources.oracle_data_source_connection import (
    OracleDataSource as OracleDataSourceModel,
)
from soda_oracle.common.data_sources.oracle_data_source_connection import (
    OracleDataSourceConnection,
)

logger: logging.Logger = soda_logger


class OracleDataSourceImpl(DataSourceImpl, model_class=OracleDataSourceModel):
    """Oracle-specific implementation of DataSourceImpl
    
    NOTE: Oracle uses a 3-part DQN format (datasource/schema/table), not a 4-part (datasource/database/schema/table).
    This is done for the following reasons:
    - Oracle does not use a database field anywhere in SQL or metadata, it is always implicit via the connection
    - The 'service name' is the database identifier, and this is sometimes but not always available in connection params
    - There are two connection methods, host/port/service_name, and connectstring.  
    - The latter passes through directly to the driver i.e. oracledb.connect(user=user, password=password, dsn=connectstring)
    - There are several different allowed formats for connectstring
        (see 4.2 https://python-oracledb.readthedocs.io/en/latest/user_guide/connection_handling.html#connection-strings)
    - It is possible to extract service name from connectstring, and an earlier draft of this code in soda-library 
    did so, but it involved a lot of string parsing and regexes and creates complexity and rigidity which is likely to bite us
    - If we drop database from DQN, we can make the connection layer very lightweight, and since
    database is not used in SQL or metadata, it doesn't affect the rest of the implementation
    - There is already a precedent for 3-part DQN in this codebase (duckDB)
    
    There is currently not a good way to tell if a data source uses 3-part or 4-part DQN aside from looking at the code.
    We considered implementing a parsing layer but you sometimes get DQNs passed in as top-level strings without 
    knowing which database engine they are written for.  We might consider extending the spec to include a protocol 
    like oracle://datasource/schema/table, but that would be a breaking change to product so we leave it for now.


    """
    def __init__(self, data_source_model: OracleDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return OracleSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return OracleDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        """Oracle does not use database prefixes like other systems"""
        return MetadataTablesQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection, prefixes=[]
        )

    def create_metadata_columns_query(self) -> MetadataColumnsQuery:
        """Oracle does not use database prefixes like other systems"""
        return MetadataColumnsQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection, prefixes=[]
        )



class OracleSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = '"'

    def get_sql_type_dict(self) -> dict[str, str]:
        """Data type that is used in the create table statement.
        This can include the length of the column, e.g. VARCHAR(255)"""
        result = self.get_contract_type_dict()
        result[DBDataType.TEXT] = "VARCHAR2(255)"
        result[DBDataType.INTEGER] = "NUMBER(10)"
        result[DBDataType.DECIMAL] = "NUMBER(10, 2)"

        return result

    def get_contract_type_dict(self) -> dict[str, str]:
        """Data type that is used in the contract.
        This does **NOT** include the length of the column, e.g. VARCHAR"""
        return {
            DBDataType.TEXT: "VARCHAR2",
            DBDataType.INTEGER: "NUMBER",
            DBDataType.DECIMAL: "NUMBER",
            DBDataType.DATE: "DATE",
            DBDataType.TIME: "TIME",
            DBDataType.TIMESTAMP: "TIMESTAMP",
            DBDataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
            DBDataType.BOOLEAN: "BOOLEAN",
        }

    def add_data_type_default_length(self, data_type: str) -> str:
        """In some cases data type metadata includes a length e.g. DATE(7) for DATE columns in Oracle.
        Return data type with default length if it exists.  Used in testing column type mismatches."""
        if data_type == self.get_contract_type_dict()[DBDataType.DATE]:
            return data_type + "(7)"
        return data_type

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        """
        Oracle does not support DISTINCT on tuples and has nothing like BigQuery's TO_JSON_STRING(STRUCT).

        Instead we approximate TO_JSON_STRING(STRUCT) by concatting all the columns.
        """

        def format_element_expression(e: str) -> str:
            """Use CHR(31) (unit seperator) as delimiter because it is highly unlikely to be appear in the data.
            If it does appear, replace with string '#US#'.  Also replace NULL with a string value to prevent
            cascading NULLS in the concat operation."""
            return f"REPLACE(NVL(TO_CHAR({e}), '__UNDEF__'), CHR(31), '#US#')"

        concat_delim = " || CHR(31) || \n"  # use ASCII unit separator as delimieter
        elements: str = concat_delim.join(
            format_element_expression(self.build_expression_sql(e)) for e in tuple.expressions
        )
        return elements

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        # Code lifted from soda-library
        return f"""
        declare
            userexist integer;
        begin
            select count(*) into userexist from dba_users where username='{schema_name}';
            if (userexist = 0) then
                execute immediate 'create user {self.quote_default(schema_name)}';
                execute immediate 'ALTER USER {self.quote_default(schema_name)} QUOTA UNLIMITED ON SYSTEM';
                execute immediate 'ALTER SESSION SET TIME_ZONE = ''+00:00''';
            end if;
        end;
        """

    def schema_information_schema(self):
        # Oracle just has top-level metadata views, no dedicated metadata schema
        return None

    def get_database_prefix_index(self) -> int | None:
        """Oracle DQNs do not include database"""
        return None

    def get_schema_prefix_index(self) -> int | None:
        """Oracle DQNs do not include database"""
        return 0

    def build_select_sql(self, select_elements: list, add_semicolon: bool = False) -> str:
        """Oracle does not use semicolons, set to False by default."""
        return super().build_select_sql(select_elements, add_semicolon)

    def _build_ordinal_position_sql(self, ordinal_position: ORDINAL_POSITION) -> str:
        return "COLUMN_ID"

    def table_tables(self) -> str:
        return "ALL_TABLES"

    def table_columns(self) -> str:
        return "ALL_TAB_COLUMNS"

    def column_table_schema(self) -> str:
        return "OWNER"

    def column_table_name(self) -> str:
        return "TABLE_NAME"

    def column_table_catalog(self) -> str:
        """Oracle does not have database catalog concept, return None to skip filtering"""
        return None

    def column_column_name(self) -> str:
        return "COLUMN_NAME"

    def column_data_type(self) -> str:
        return "DATA_TYPE"

    def column_data_type_max_length(self) -> str:
        return "DATA_LENGTH"

    def literal_date(self, date_value) -> str:
        """Oracle-specific date literal format"""
        date_string = date_value.strftime("%Y-%m-%d")
        return f"DATE'{date_string}'".strip()

    def literal_datetime(self, datetime_value) -> str:
        """Oracle-specific timestamp literal format"""
        if datetime_value.tzinfo:
            datetime_str = datetime_value.strftime("%Y-%m-%d %H:%M:%S %z")
            datetime_str_formatted = datetime_str[:-2] + ":" + datetime_str[-2:]
        else:
            datetime_str_formatted = datetime_value.strftime("%Y-%m-%d %H:%M:%S")

        return f"TIMESTAMP '{datetime_str_formatted}'".strip()

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        """Oracle-specific date truncation using TRUNC"""
        return f"TRUNC({timestamp_literal}, 'DD')"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        """Oracle uses all caps"""
        return f"{timestamp_literal} + INTERVAL '1' DAY"

    def sql_expr_timestamp_with_tz_literal(self, datetime_in_iso8601: str) -> str:
        """Oracle-specific timestamp literal with timezone support"""
        return f"TO_TIMESTAMP_TZ('{datetime_in_iso8601}', 'YYYY-MM-DD\"T\"HH24:MI:SS\"+\"TZH:TZM')"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        """Oracle-specific timestamp literal with timezone support"""
        return f"TIMESTAMP '{datetime_in_iso8601}'"

    def _alias_format(self, alias: str) -> str:
        """No "AS" in Oracle"""
        return self.quote_default(alias)
