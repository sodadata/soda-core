import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import (
    COLUMN,
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    INSERT_INTO,
    LENGTH,
    REGEX_LIKE,
    TUPLE,
)
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_core.common.statements.metadata_columns_query import (
    ColumnMetadata,
    MetadataColumnsQuery,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SQLServerDataSource as SQLServerDataSourceModel,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SQLServerDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SQLServerDataSourceImpl(DataSourceImpl, model_class=SQLServerDataSourceModel):
    def __init__(self, data_source_model: SQLServerDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SQLServerSqlDialect(self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SQLServerDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SQLServerSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "["  # Do not use this! Always use quote_default()

    def __init__(self, data_source_impl: SQLServerDataSourceImpl):
        # We need to pass the data_source_impl to the SqlDialect to be able to use the metadata query.
        super().__init__()
        self.data_source_impl = data_source_impl

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return f"[{identifier}]" if isinstance(identifier, str) and len(identifier) > 0 else None

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        return f"""
        IF NOT EXISTS ( SELECT  *
                        FROM    sys.schemas
                        WHERE   name = N'{schema_name}' )
        EXEC('CREATE SCHEMA [{schema_name}]');
        """

    def build_drop_table_sql(self, drop_table: DROP_TABLE | DROP_TABLE_IF_EXISTS, add_semicolon: bool = True) -> str:
        if_exists_sql: str = (
            f"IF OBJECT_ID('{drop_table.fully_qualified_table_name}', 'U') IS NOT NULL"
            if isinstance(drop_table, DROP_TABLE_IF_EXISTS)
            else ""
        )
        return f"{if_exists_sql} DROP TABLE {drop_table.fully_qualified_table_name}" + (";" if add_semicolon else "")

    def _build_create_table_statement_sql(self, create_table: CREATE_TABLE | CREATE_TABLE_IF_NOT_EXISTS) -> str:
        if_not_exists_sql: str = (
            f"IF OBJECT_ID('{create_table.fully_qualified_table_name}', 'U') IS NULL"
            if isinstance(create_table, CREATE_TABLE_IF_NOT_EXISTS)
            else ""
        )
        create_table_sql: str = f"{if_not_exists_sql} CREATE TABLE {create_table.fully_qualified_table_name} "
        return create_table_sql

    def _build_insert_into_columns_sql(self, insert_into: INSERT_INTO) -> str:
        columns = insert_into.columns
        if columns is None:
            columns = self.__get_columns_for_table(insert_into.fully_qualified_table_name)
        columns_sql: str = " (" + ", ".join([self.build_expression_sql(column) for column in columns]) + ")"
        return columns_sql

    def __get_columns_for_table(self, table_name: str) -> list[COLUMN]:
        # We do not have access to the DataSourceImpl here, so we cannot use the metadata query. (TODO: do we move this creation to the SQLDialect?)
        # First parse the catalog, schema and table name from the fully qualified table name.
        splitted_name: list[str] = [
            table_name[1:-1] for table_name in table_name.split(".")
        ]  # Remove the brackets (i.e. quotes).
        if len(splitted_name) == 2:
            schema, table_name = splitted_name
            # This feels like a hack, but it works.
            # We have no other way of getting the database name here.
            dataset_prefixes = [self.data_source_impl.data_source_connection.connection_properties.database, schema]
        else:
            catalog, schema, table_name = splitted_name
            dataset_prefixes = [catalog, schema]

        # The metadata query uses ordinal position to order the columns.
        # This is the order that the values should be inserted as well!
        metadata_columns_query: MetadataColumnsQuery = self.data_source_impl.create_metadata_columns_query()
        sql: str = metadata_columns_query.build_sql(dataset_prefix=dataset_prefixes, dataset_name=table_name)
        result: QueryResult = self.data_source_impl.execute_query(sql)
        columns: list[ColumnMetadata] = metadata_columns_query.get_result(result)

        columns_to_return: list[COLUMN] = [COLUMN(column.column_name) for column in columns]

        return columns_to_return

    def _does_datasource_require_columns_for_insert(self) -> bool:
        return True

    def _build_length_sql(self, length: LENGTH) -> str:
        return f"LEN({self.build_expression_sql(length.expression)})"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"'{datetime_in_iso8601}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETRUNC(DAY, {timestamp_literal})"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"DATEADD(DAY, 1, {timestamp_literal})"

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return f"CHECKSUM{super()._build_tuple_sql(tuple)}"
        return super()._build_tuple_sql(tuple)

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"PATINDEX ('{matches.regex_pattern}', {expression}) > 0"

    def supports_regex_advanced(self) -> bool:
        return False

    def get_sql_type_dict(self) -> dict[str, str]:
        base_dict = super().get_sql_type_dict()
        base_dict[DBDataType.TEXT] = "varchar(255)"
        return base_dict

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            DBDataType.TEXT: "varchar",
            DBDataType.INTEGER: "int",
            DBDataType.DECIMAL: "float",
            DBDataType.DATE: "date",
            DBDataType.TIME: "time",
            DBDataType.TIMESTAMP: "datetime",
            DBDataType.TIMESTAMP_TZ: "datetimeoffset",
            DBDataType.BOOLEAN: "bit",
        }
