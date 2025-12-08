import logging
from datetime import date, datetime
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    AND,
    COLUMN,
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_AS_SELECT,
    CREATE_TABLE_IF_NOT_EXISTS,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    FROM,
    INSERT_INTO,
    INSERT_INTO_VIA_SELECT,
    INTO,
    LENGTH,
    LIMIT,
    OFFSET,
    ORDER_BY_ASC,
    REGEX_LIKE,
    SELECT,
    STAR,
    STRING_HASH,
    TUPLE,
    VALUES,
    WHERE,
    WITH,
    SqlExpressionStr,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSource as SqlServerDataSourceModel,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SqlServerDataSourceImpl(DataSourceImpl, model_class=SqlServerDataSourceModel):
    def __init__(self, data_source_model: SqlServerDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return SqlServerSqlDialect(data_source_impl=self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SqlServerDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SqlServerSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "["  # Do not use this! Always use quote_default()
    SODA_DATA_TYPE_SYNONYMS = ((SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR),)

    def build_select_sql(self, select_elements: list, add_semicolon: bool = True) -> str:
        statement_lines: list[str] = []
        statement_lines.extend(self._build_cte_sql_lines(select_elements))
        statement_lines.extend(self._build_select_sql_lines(select_elements))
        statement_lines.extend(self._build_into_sql_lines(select_elements))
        statement_lines.extend(self._build_from_sql_lines(select_elements))
        statement_lines.extend(self._build_where_sql_lines(select_elements))
        statement_lines.extend(self._build_group_by_sql_lines(select_elements))
        statement_lines.extend(self._build_order_by_lines(select_elements))

        offset_line = self._build_offset_line(select_elements)
        if offset_line:
            statement_lines.append(offset_line)

        limit_line = self._build_limit_line(select_elements)
        if limit_line:
            if not limit_line.startswith("__SODA_LIMIT_TOP__"):
                statement_lines.append(limit_line)
            else:  # We should add the TOP statement to the SELECT statement.
                # Get the last SELECT line and add the TOP statement to it.
                last_select_line: str = [line for line in statement_lines if line.startswith("SELECT")][-1]
                last_select_line_with_top = last_select_line.replace(
                    "SELECT ", f"SELECT TOP {limit_line.split('__SODA_LIMIT_TOP__')[1]} "
                )
                # Find the index of the last SELECT line and replace it with the new line.
                last_select_line_index = statement_lines.index(last_select_line)
                statement_lines[last_select_line_index] = last_select_line_with_top

        return "\n".join(statement_lines) + (";" if add_semicolon else "")

    def _build_limit_line(self, select_elements: list) -> Optional[str]:
        # First, check if there is a LIMIT statement in the select elements.
        limit_statement_present = any(isinstance(select_element, LIMIT) for select_element in select_elements)
        if not limit_statement_present:
            return None

        # Check if there is an OFFSET statement in the select elements. If so, use the default logic.
        uses_offset = any(isinstance(select_element, OFFSET) for select_element in select_elements)
        if uses_offset:
            return super()._build_limit_line(select_elements)

        # If no OFFSET is used, but there is a LIMIT, we should use TOP instead of FETCH NEXT.
        limit_element: LIMIT = [
            select_element for select_element in select_elements if isinstance(select_element, LIMIT)
        ][0]
        return f"__SODA_LIMIT_TOP__{limit_element.limit}"  # Use a custom keyword to indicate that we are using TOP instead of FETCH NEXT.

    def literal_date(self, date: date):
        """Technically dates can be passed directly as strings, but this is more explicit."""
        date_string = date.strftime("%Y-%m-%d")
        return f"CAST('{date_string}' AS DATE)"

    def literal_datetime(self, datetime: datetime):
        return f"'{datetime.isoformat(timespec='milliseconds')}'"

    def literal_boolean(self, boolean: bool):
        return "1" if boolean is True else "0"

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return f"[{identifier}]" if isinstance(identifier, str) and len(identifier) > 0 else None

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        schema_name: str = prefixes[1]
        return f"""
        IF NOT EXISTS ( SELECT  *
                        FROM    sys.schemas
                        WHERE   name = N'{schema_name}' )
        EXEC('CREATE SCHEMA [{schema_name}]')
        """ + (
            ";" if add_semicolon else ""
        )

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
        if tuple.check_context(VALUES):
            # in built_cte_values_sql, elements are dropped in top-level select statement, so can't use parentheses
            return ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
        return super()._build_tuple_sql(tuple)

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        regex_pattern = matches.regex_pattern
        # alpha expansion doesn't work properly for case sensitive ranges in SQLServer
        # this is quite a hack to fit the common use-cases.  generally regex's are only partially supported anyway
        regex_pattern = regex_pattern.replace("a-z", "abcdefghijklmnopqrstuvwxyz")
        regex_pattern = regex_pattern.replace("A-Z", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        # collations define rules for sorting strings and distinguishing similar characters
        # see: https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver17
        # CS: Case sensitive; AS: Accent sensitive
        # The default is SQL_Latin1_General_Cp1_CI_AS (case-insensitive), we replcae with a case sensitive collation
        return f"PATINDEX ('%{regex_pattern}%', {expression} COLLATE SQL_Latin1_General_Cp1_CS_AS) > 0"

    def supports_regex_advanced(self) -> bool:
        return False

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return "\nUNION ALL\n".join(["SELECT " + self.build_expression_sql(value) for value in values.values])

    def select_all_paginated_sql(
        self,
        dataset_identifier: DatasetIdentifier,
        columns: list[str],
        filter: Optional[str],
        order_by: list[str],
        limit: int,
        offset: int,
    ) -> str:
        where_clauses = []

        if filter:
            where_clauses.append(SqlExpressionStr(filter))

        statements = [
            SELECT(columns or [STAR()]),
            FROM(table_name=dataset_identifier.dataset_name, table_prefix=dataset_identifier.prefixes),
            WHERE.optional(AND.optional(where_clauses)),
            *[ORDER_BY_ASC(c) for c in order_by],
            OFFSET(offset),
            LIMIT(limit),
        ]

        return self.build_select_sql(statements)

    def _build_limit_sql(self, limit_element: LIMIT) -> str:
        return f"FETCH NEXT {limit_element.limit} ROWS ONLY"

    def _build_offset_sql(self, offset_element: OFFSET) -> str:
        return f"OFFSET {offset_element.offset} ROWS"

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", "nvarchar"],
            ["char", "nchar"],
            ["int", "integer"],
            ["bigint"],
            ["smallint"],
            ["real"],
            ["float", "double precision"],
            ["datetime2", "datetime"],
        ]

    # copied from redshift
    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "varchar",
            SodaDataTypeName.SMALLINT: "smallint",  #
            SodaDataTypeName.INTEGER: "int",  #
            SodaDataTypeName.BIGINT: "bigint",  #
            SodaDataTypeName.NUMERIC: "numeric",  #
            SodaDataTypeName.DECIMAL: "decimal",  #
            SodaDataTypeName.FLOAT: "real",  #
            SodaDataTypeName.DOUBLE: "float",
            SodaDataTypeName.TIMESTAMP: "datetime2",
            SodaDataTypeName.TIMESTAMP_TZ: "datetimeoffset",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "bit",
        }

    # copied from redshift
    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            # Character types
            "char": SodaDataTypeName.CHAR,
            "varchar": SodaDataTypeName.VARCHAR,
            "text": SodaDataTypeName.TEXT,
            "nchar": SodaDataTypeName.CHAR,
            "nvarchar": SodaDataTypeName.VARCHAR,
            "ntext": SodaDataTypeName.TEXT,
            # Integer types
            "tinyint": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "int": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            # Exact numeric types
            "numeric": SodaDataTypeName.NUMERIC,
            "decimal": SodaDataTypeName.DECIMAL,
            # Approximate numeric types
            "real": SodaDataTypeName.FLOAT,
            "float": SodaDataTypeName.DOUBLE,
            # Date/time types
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "datetime2": SodaDataTypeName.TIMESTAMP,
            "datetimeoffset": SodaDataTypeName.TIMESTAMP_TZ,
            "datetime": SodaDataTypeName.TIMESTAMP,
            "smalldatetime": SodaDataTypeName.TIMESTAMP,
            # Boolean type
            "bit": SodaDataTypeName.BOOLEAN,
        }

    def supports_data_type_character_maximum_length(self) -> bool:
        return True

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def supports_data_type_datetime_precision(self) -> bool:
        return True

    def supports_datetime_microseconds(self) -> bool:
        return False

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", "nvarchar", "nchar"]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "decimal", "float"]

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "decimal"]

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in [
            "time",
            "datetime2",
            "datetimeoffset",
        ]

    def default_varchar_length(self) -> Optional[int]:
        return 255

    def is_quoted(self, identifier: str) -> bool:
        return identifier.startswith("[") and identifier.endswith("]")

    def build_insert_into_sql(self, insert_into: INSERT_INTO, add_semicolon: bool = True) -> str:
        # SqlServer supports a max of 1000 rows in an insert statement. If that's the case, split the insert into multiple statements and recursively call this function.
        STEP_SIZE = self.get_preferred_number_of_rows_for_insert()
        if len(insert_into.values) > STEP_SIZE:
            final_insert_sql = ""
            for i in range(0, len(insert_into.values), STEP_SIZE):
                temp_insert_into = INSERT_INTO(
                    fully_qualified_table_name=insert_into.fully_qualified_table_name,
                    columns=insert_into.columns,
                    values=insert_into.values[i : i + STEP_SIZE],
                )
                final_insert_sql += self.build_insert_into_sql(
                    temp_insert_into, add_semicolon=True
                )  # Now we force the semicolon to separate the statements
                final_insert_sql += "\n"
            return final_insert_sql

        return super().build_insert_into_sql(insert_into, add_semicolon=add_semicolon)

    def build_insert_into_via_select_sql(
        self, insert_into_via_select: INSERT_INTO_VIA_SELECT, add_semicolon: bool = True
    ) -> str:
        # First get all the WITH clauses from the select elements.
        with_clauses: list[str] = []
        remaining_select_elements: list[str] = []
        for select_element in insert_into_via_select.select_elements:
            if isinstance(select_element, WITH):
                with_clauses.append(select_element)
            else:  # Split of the other elements
                remaining_select_elements.append(select_element)
        # Then build the with statements.
        with_statements: str = "\n".join(self._build_cte_sql_lines(with_clauses))
        insert_into_sql: str = f"{with_statements}\nINSERT INTO {insert_into_via_select.fully_qualified_table_name}\n"
        insert_into_sql += self._build_insert_into_columns_sql(insert_into_via_select) + "\n"
        insert_into_sql += "(\n" + self.build_select_sql(remaining_select_elements, add_semicolon=False) + "\n)"
        return insert_into_sql + (";" if add_semicolon else "")

    def get_preferred_number_of_rows_for_insert(self) -> int:
        return 1000

    def map_test_sql_data_type_to_data_source(self, source_data_type: SqlDataType) -> SqlDataType:
        """SQLServer always requires a varchar length in create table statements."""
        sql_data_type = super().map_test_sql_data_type_to_data_source(source_data_type)
        if sql_data_type.name == "varchar" and sql_data_type.character_maximum_length is None:
            sql_data_type.character_maximum_length = self.default_varchar_length()
        return sql_data_type

    @classmethod
    def is_same_soda_data_type_with_synonyms(cls, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        if expected == SodaDataTypeName.CHAR and actual == SodaDataTypeName.VARCHAR:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, expected {expected} and actual {actual} are treated as the same because of SQLServer cursor not distinguishing between varchar and char"
            )
            return True
        elif expected == SodaDataTypeName.NUMERIC and actual == SodaDataTypeName.DECIMAL:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, expected {expected} and actual {actual} are treated as the same because of SQLServer cursor not distinguishing between numeric and decimal"
            )
            return True
        elif expected == SodaDataTypeName.TIMESTAMP_TZ and actual == SodaDataTypeName.VARCHAR:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, expected {expected} and actual {actual} are treated as the same because of SQLServer cursor returns varchar for timestamps with timezone"
            )
            return True
        return super().is_same_soda_data_type_with_synonyms(expected, actual)

    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        return f"CONVERT(VARCHAR(32), HASHBYTES('MD5', {self.build_expression_sql(string_hash.expression)}), 2)"

    def _get_add_column_sql_expr(self) -> str:
        return "ADD"

    def build_create_table_as_select_sql(
        self, create_table_as_select: CREATE_TABLE_AS_SELECT, add_semicolon: bool = True, add_paranthesis: bool = True
    ) -> str:
        # Copy the select elements and insert an INTO with the same table name as the create table as select statement
        select_elements = create_table_as_select.select_elements.copy()
        select_elements += [INTO(fully_qualified_table_name=create_table_as_select.fully_qualified_table_name)]
        result_sql: str = self.build_select_sql(select_elements, add_semicolon=add_semicolon)
        return result_sql
