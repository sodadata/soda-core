from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import date, datetime, time
from numbers import Number
from textwrap import indent
from typing import TYPE_CHECKING, Any, Optional, Tuple

from soda_core.common.data_source_results import QueryResult
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import (
    ColumnMetadata,
    DataSourceNamespace,
    SodaDataTypeName,
    SqlDataType,
)
from soda_core.common.soda_cloud_dto import SamplerType
from soda_core.common.sql_ast import (
    ALTER_TABLE,
    ALTER_TABLE_ADD_COLUMN,
    ALTER_TABLE_DROP_COLUMN,
    AND,
    CASE_WHEN,
    CAST,
    COALESCE,
    COLUMN,
    COMBINED_HASH,
    CONCAT,
    CONCAT_WS,
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_AS_SELECT,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CTE,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    EQ,
    EXISTS,
    FROM,
    FUNCTION,
    GROUP_BY,
    GT,
    GTE,
    IN,
    IN_SELECT,
    INSERT_INTO,
    INSERT_INTO_VIA_SELECT,
    IS_NOT_NULL,
    IS_NULL,
    JOIN,
    LEFT_INNER_JOIN,
    LENGTH,
    LIKE,
    LIMIT,
    LITERAL,
    LOWER,
    LT,
    LTE,
    MAX,
    NEQ,
    NOT,
    NOT_LIKE,
    OFFSET,
    OR,
    ORDER_BY_ASC,
    ORDER_BY_DESC,
    ORDINAL_POSITION,
    REGEX_LIKE,
    SELECT,
    STAR,
    STRING_HASH,
    SUM,
    TUPLE,
    VALUES,
    VALUES_ROW,
    WHERE,
    WITH,
    Operator,
    SqlExpression,
    SqlExpressionStr,
)
from soda_core.common.sql_utils import apply_sampling_to_sql

if TYPE_CHECKING:
    from soda_core.common.data_source_impl import DataSourceImpl

logger: logging.Logger = soda_logger


class SqlDialect:
    """
    Extends DataSource with all logic to builds the SQL queries.
    Specific DataSource's can customize their SQL queries by subclassing SqlDialect,
    overriding methods of SqlDialect and returning the customized SqlDialect in DataSource._create_sql_dialect()
    """

    DEFAULT_QUOTE_CHAR = '"'

    SODA_DATA_TYPE_SYNONYMS: tuple[tuple[SodaDataTypeName, ...]] = ()

    def __init__(
        self,
        data_source_impl: DataSourceImpl,
    ):
        self.data_source_impl: DataSourceImpl = data_source_impl

        self._data_type_name_synonym_mappings: dict[str, str] = self._build_data_type_name_synonym_mappings(
            self._get_data_type_name_synonyms()
        )

    # Data type handling

    def _build_data_type_name_synonym_mappings(self, data_type_name_synonyms: list[list[str]]) -> dict[str, str]:
        data_type_name_synonym_mappings: dict[str, str] = {}
        for data_type_name_synonym_list in data_type_name_synonyms:
            first_type_lower: str = data_type_name_synonym_list[0].lower()
            for data_type_name_synonym in data_type_name_synonym_list:
                data_type_name_synonym_mappings[data_type_name_synonym.lower()] = first_type_lower
        return data_type_name_synonym_mappings

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        # Implements data type synonyms
        # Each list should represent a list of synonyms
        return [
            # Eg for postgres
            # ["varchar", "character varying"],
            # ["char", "character"],
            # ["integer", "int", "int4"],
            # ["bigint", "int8"],
            # ["smallint", "int2"],
            # ["real", "float4"],
            # ["double precision", "float8"],
        ]

    def data_type_names_are_same_or_synonym(self, left_data_type_name: str, right_data_type_name: str) -> bool:
        left_data_type_name_lower: str = left_data_type_name.lower()
        right_data_type_name_lower: str = right_data_type_name.lower()
        if left_data_type_name_lower == right_data_type_name_lower:
            return True
        left_synonym_data_type_name: str = self._data_type_name_synonym_mappings.get(
            left_data_type_name_lower, left_data_type_name_lower
        )
        right_synonym_data_type_name: str = self._data_type_name_synonym_mappings.get(
            right_data_type_name_lower, right_data_type_name_lower
        )
        return left_synonym_data_type_name == right_synonym_data_type_name

    def get_data_source_data_type_name_for_soda_data_type_name(self, soda_data_type_name: SodaDataTypeName) -> str:
        return self.get_data_source_data_type_name_by_soda_data_type_names()[soda_data_type_name]

    @abstractmethod
    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[SodaDataTypeName, str]:
        """
        Maps SodaDataTypeName's names to native data source type names.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        """
        Maps native data source type names to SodaDataTypeName's
        """
        raise NotImplementedError()

    def is_same_data_type_for_dwh_column(self, expected: SqlDataType, actual: SqlDataType):
        return self.is_same_data_type_for_schema_check(expected=expected, actual=actual)

    def is_same_data_type_for_schema_check(self, expected: SqlDataType, actual: SqlDataType):
        if not self.data_type_names_are_same_or_synonym(expected.name, actual.name):
            return False
        if (
            isinstance(expected.character_maximum_length, int)
            and expected.character_maximum_length != actual.character_maximum_length
        ):
            return False
        if isinstance(expected.numeric_precision, int) and expected.numeric_precision != actual.numeric_precision:
            return False
        if isinstance(expected.numeric_scale, int) and expected.numeric_scale != actual.numeric_scale:
            return False
        if isinstance(expected.datetime_precision, int) and expected.datetime_precision != actual.datetime_precision:
            return False
        return True

    def get_synonyms_for_soda_data_type(self) -> list[list[SodaDataTypeName]]:
        # This function can be overloaded if required.
        # It could be that the datasource has synonyms for the data types, and we want to handle that in the mappings.
        # For an example: see the postgres implementation. We basically create a list of lists. Whereby each sublist contains the synonyms for a given SodaDataTypeName.
        return []

    @classmethod
    def is_same_soda_data_type_with_synonyms(cls, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        if expected == actual:
            return True

        for synonyms in cls.SODA_DATA_TYPE_SYNONYMS:
            if expected in synonyms and actual in synonyms:
                logger.debug(
                    f"In is_same_soda_data_type_with_synonyms, expected {expected} and actual {actual} are treated as the same"
                )
                return True

        return False

    def map_test_sql_data_type_to_data_source(self, source_data_type: SqlDataType) -> SqlDataType:
        test_data_type: str = source_data_type.name
        data_type_name: str = self.get_data_source_data_type_name_by_soda_data_type_names().get(test_data_type)
        character_maximum_length: Optional[int] = (
            source_data_type.character_maximum_length if self.supports_data_type_character_maximum_length() else None
        )
        numeric_precision: Optional[int] = (
            source_data_type.numeric_precision if self.supports_data_type_numeric_precision() else None
        )
        numeric_scale: Optional[int] = (
            source_data_type.numeric_scale if self.supports_data_type_numeric_scale() else None
        )
        datetime_precision: Optional[int] = (
            source_data_type.datetime_precision if self.supports_data_type_datetime_precision() else None
        )
        return self.get_sql_data_type_class()(
            name=data_type_name,
            character_maximum_length=character_maximum_length,
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
            datetime_precision=datetime_precision,
        )

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "varchar",
            "char",
            "character varying",
            "character",
        ]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "timestamp",
            "timestamp without time zone",
            "timestamptz",
            "timestamp with time zone",
            "time",
            "time with time zone",
        ]

    # SQL generation

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return (
            f"{self.DEFAULT_QUOTE_CHAR}{identifier}{self.DEFAULT_QUOTE_CHAR}"
            if isinstance(identifier, str) and len(identifier) > 0
            else None
        )

    def build_fully_qualified_sql_name(self, dataset_identifier: DatasetIdentifier) -> str:
        return self.qualify_dataset_name(
            dataset_prefix=dataset_identifier.prefixes, dataset_name=dataset_identifier.dataset_name
        )

    def qualify_dataset_name(self, dataset_prefix: list[str], dataset_name: str) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        parts: list[str] = list(dataset_prefix) if dataset_prefix else []
        parts.append(dataset_name)
        parts = [self.quote_default(p) for p in parts if p]
        return ".".join(parts)

    def literal(self, o: object) -> str:
        if o is None:
            return "NULL"
        elif isinstance(o, Number) and not isinstance(
            o, bool
        ):  # We don't want to interpret booleans as numbers. In Python a boolean is a subclass of int, so we have to explicitly check it
            return self.literal_number(o)
        elif isinstance(o, str):
            return self.literal_string(o)
        elif isinstance(o, datetime):
            if o.tzinfo is None:
                return self.literal_datetime(o)
            else:
                return self.literal_datetime_with_tz(o)
        elif isinstance(o, time):
            return self.literal_time(o)
        elif isinstance(o, date):
            return self.literal_date(o)
        elif isinstance(o, list) or isinstance(o, set) or isinstance(o, tuple):
            return self.literal_list(o)
        elif isinstance(o, bool):
            return self.literal_boolean(o)
        elif isinstance(o, LITERAL):  # If someone passes a LITERAL object, we want to use the value
            return self.literal(o.value)
        raise RuntimeError(f"Cannot convert type {type(o)} to a SQL literal: {o}")

    def literal_number(self, value: Number):
        if value is None:
            return None
        return str(value)

    def literal_string(self, value: str) -> Optional[str]:
        if value is None:
            return None
        return "'" + self.escape_string(value) + "'"

    def literal_list(self, l: list):
        if l is None:
            return None
        return "(" + (",".join([self.literal(e) for e in l])) + ")"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE '{date_string}'"

    def literal_datetime(self, datetime: datetime):
        return f"'{datetime.isoformat()}'"

    def literal_time(self, time: time):
        time_str: str = time.strftime("%H:%M:%S.%f")
        return f"'{time_str}'"

    def literal_datetime_with_tz(self, datetime: datetime):
        # Can be overloaded if the subclass does not support timezones (may have to do conversion yourself)
        # We assume that all timestamps are stored in UTC.
        # See Fabric for an example
        return self.literal_datetime(datetime)

    def literal_boolean(self, boolean: bool):
        return "TRUE" if boolean is True else "FALSE"

    def escape_string(self, value: str):
        # string_literal: str = re.sub(r"(\\.)", r"\\\1", value)
        string_literal: str = value.replace("'", "''")
        return string_literal

    def escape_regex(self, value: str):
        return value

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        assert len(prefixes) == 2, f"Expected 2 prefixes, got {len(prefixes)}"
        schema_name: str = prefixes[1]
        quoted_schema_name: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {quoted_schema_name}" + (";" if add_semicolon else "")

    def post_schema_create_sql(self, prefixes: list[str]) -> Optional[list[str]]:
        """Optional list of SQL commands to execute after schema is created (e.g. to set permissions)"""
        return None

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
            LIMIT(limit),
            OFFSET(offset),
        ]

        return self.build_select_sql(statements)

    #########################################################
    # CREATE TABLE
    #########################################################
    def build_create_table_sql(
        self, create_table: CREATE_TABLE | CREATE_TABLE_IF_NOT_EXISTS, add_semicolon: bool = True
    ) -> str:
        create_table_sql = self._build_create_table_statement_sql(create_table)

        create_table_sql = (
            create_table_sql
            + "(\n"
            + ",\n".join([self._build_create_table_column(column) for column in create_table.columns])
            + "\n)"
        )
        return create_table_sql + (";" if add_semicolon else "")

    def _build_create_table_statement_sql(self, create_table: CREATE_TABLE | CREATE_TABLE_IF_NOT_EXISTS) -> str:
        if_not_exists_sql: str = "IF NOT EXISTS" if isinstance(create_table, CREATE_TABLE_IF_NOT_EXISTS) else ""
        create_table_sql: str = f"CREATE TABLE {if_not_exists_sql} {create_table.fully_qualified_table_name} "
        return create_table_sql

    def _build_create_table_column(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        column_name_quoted: str = self._quote_column_for_create_table(create_table_column.name)
        column_type_sql: str = self._build_create_table_column_type(create_table_column)

        is_nullable_sql: str = (
            " NOT NULL" if (create_table_column.nullable is False and self._is_not_null_ddl_supported()) else ""
        )
        default_sql: str = (
            f" DEFAULT {self.literal(create_table_column.default)}" if create_table_column.default else ""
        )

        return f"\t{column_name_quoted} {column_type_sql}{is_nullable_sql}{default_sql}"

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        assert isinstance(create_table_column.type, SqlDataType)

        if not self.supports_data_type_character_maximum_length():
            create_table_column.type.character_maximum_length = None
        if not self.supports_data_type_numeric_precision():
            create_table_column.type.numeric_precision = None
        if not self.supports_data_type_numeric_scale():
            create_table_column.type.numeric_scale = None
        if not self.supports_data_type_datetime_precision():
            create_table_column.type.datetime_precision = None

        return create_table_column.type.get_sql_data_type_str_with_parameters()

    def _quote_column_for_create_table(self, column_name: str) -> str:
        return self.quote_default(
            column_name
        )  # Some datasources (Athena) require a different quoting when creating a table.

    def _is_not_null_ddl_supported(self) -> bool:
        return True

    def build_create_table_as_select_sql(
        self, create_table_as_select: CREATE_TABLE_AS_SELECT, add_semicolon: bool = True
    ) -> str:
        raise NotImplementedError(
            "This method (build_create_table_as_select_sql) should be overwritten by the data source dialect"
        )

    #########################################################
    # ALTER TABLE
    #########################################################
    def build_alter_table_sql(self, alter_table: ALTER_TABLE, add_semicolon: bool = True) -> str:
        if isinstance(alter_table, ALTER_TABLE_ADD_COLUMN):
            return self._build_alter_table_add_column_sql(alter_table, add_semicolon)
        elif isinstance(alter_table, ALTER_TABLE_DROP_COLUMN):
            return self._build_alter_table_drop_column_sql(alter_table, add_semicolon)
        else:
            raise ValueError(f"Unexpected alter table type: {alter_table.__class__.__name__}")

    def _build_alter_table_add_column_sql(
        self, alter_table: ALTER_TABLE_ADD_COLUMN, add_semicolon: bool = True, add_paranthesis: bool = False
    ) -> str:
        column_name_quoted: str = self._quote_column_for_create_table(alter_table.column.name)
        column_type_sql: str = self._build_create_table_column_type(alter_table.column)
        is_nullable_sql: str = (
            " NOT NULL" if (alter_table.column.nullable is False and self._is_not_null_ddl_supported()) else ""
        )
        default_sql: str = f" DEFAULT {self.literal(alter_table.column.default)}" if alter_table.column.default else ""
        pre_paranthesis_sql: str = "(" if add_paranthesis else ""
        post_paranthesis_sql: str = ")" if add_paranthesis else ""
        return (
            f"ALTER TABLE {alter_table.fully_qualified_table_name} {self._get_add_column_sql_expr()} {pre_paranthesis_sql}{column_name_quoted} {column_type_sql}{is_nullable_sql}{default_sql}{post_paranthesis_sql}"
            + (";" if add_semicolon else "")
        )

    def _get_add_column_sql_expr(self) -> str:
        return "ADD COLUMN"

    def _build_alter_table_drop_column_sql(
        self, alter_table: ALTER_TABLE_DROP_COLUMN, add_semicolon: bool = True
    ) -> str:
        column_name_quoted: str = self._quote_column_for_create_table(alter_table.column_name)
        return f"ALTER TABLE {alter_table.fully_qualified_table_name} DROP COLUMN {column_name_quoted}" + (
            ";" if add_semicolon else ""
        )

    def drop_column_supported(self) -> bool:
        return True

    #########################################################
    # DROP TABLE
    #########################################################
    def build_drop_table_sql(self, drop_table: DROP_TABLE | DROP_TABLE_IF_EXISTS, add_semicolon: bool = True) -> str:
        if_exists_sql: str = "IF EXISTS " if isinstance(drop_table, DROP_TABLE_IF_EXISTS) else ""
        return f"DROP TABLE {if_exists_sql}{drop_table.fully_qualified_table_name}" + (";" if add_semicolon else "")

    #########################################################
    # INSERT INTO
    #########################################################
    def build_insert_into_sql(self, insert_into: INSERT_INTO, add_semicolon: bool = True) -> str:
        insert_into_sql: str = f"INSERT INTO {insert_into.fully_qualified_table_name}"
        insert_into_sql += self._build_insert_into_columns_sql(insert_into)
        insert_into_sql += self._build_insert_into_values_sql(insert_into)
        return insert_into_sql + (";" if add_semicolon else "")

    def _build_insert_into_columns_sql(self, insert_into: INSERT_INTO) -> str:
        columns_sql: str = " (" + ", ".join([self.build_expression_sql(column) for column in insert_into.columns]) + ")"
        return columns_sql

    def build_insert_into_via_select_sql(
        self, insert_into_via_select: INSERT_INTO_VIA_SELECT, add_semicolon: bool = True
    ) -> str:
        insert_into_sql: str = f"INSERT INTO {insert_into_via_select.fully_qualified_table_name}\n"
        insert_into_sql += self._build_insert_into_columns_sql(insert_into_via_select) + "\n"
        insert_into_sql += (
            "(\n" + self.build_select_sql(insert_into_via_select.select_elements, add_semicolon=False) + "\n)"
        )
        return insert_into_sql + (";" if add_semicolon else "")

    def _build_insert_into_values_sql(self, insert_into: INSERT_INTO) -> str:
        values_sql: str = " VALUES\n" + ",\n".join(
            [self._build_insert_into_values_row_sql(value) for value in insert_into.values]
        )
        return values_sql

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return " VALUES\n" + ",\n".join([self.build_expression_sql(value) for value in values.values])

    def _build_insert_into_values_row_sql(self, values: VALUES_ROW) -> str:
        values_sql: str = "(" + ", ".join([self.literal(value) for value in values.values]) + ")"
        # We used to have this line here to escape special characters.
        # But this doesn't accurately insert text fields with escaped characters (such as newlines): values_sql = self.encode_string_for_sql(values_sql)
        return values_sql

    #########################################################
    # SELECT
    #########################################################

    # TODO: refactor this to use AST (`SELECT`) instead of a list of `select_elements`. See inherited overriden methods as well.
    def build_select_sql(self, select_elements: list, add_semicolon: bool = True) -> str:
        statement_lines: list[str] = []
        statement_lines.extend(self._build_cte_sql_lines(select_elements))
        statement_lines.extend(self._build_select_sql_lines(select_elements))
        statement_lines.extend(self._build_from_sql_lines(select_elements))
        statement_lines.extend(self._build_where_sql_lines(select_elements))
        statement_lines.extend(self._build_group_by_sql_lines(select_elements))
        statement_lines.extend(self._build_order_by_lines(select_elements))

        limit_line = self._build_limit_line(select_elements)
        if limit_line:
            statement_lines.append(limit_line)

        offset_line = self._build_offset_line(select_elements)
        if offset_line:
            statement_lines.append(offset_line)
        return "\n".join(statement_lines) + (";" if add_semicolon else "")

    def _build_select_sql_lines(self, select_elements: list) -> list[str]:
        select_field_sqls: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, SELECT):
                if isinstance(select_element.fields, str) or isinstance(select_element.fields, SqlExpression):
                    select_element.fields = [select_element.fields]
                for select_field in select_element.fields:
                    if isinstance(select_field, str):
                        select_field_sqls.append(self.quote_default(select_field))
                    elif isinstance(select_field, SqlExpression):
                        select_field_sqls.append(self.build_expression_sql(select_field))
                    else:
                        raise Exception(f"Invalid select field type: {select_field.__class__.__name__}")

        # Alternatively, concatenate all the fields on one line to reduce SQL statement length
        # return "SELECT " + (", ".join(select_fields_sql))
        # For now, we opt for SELECT statement readability...

        select_sql_lines: list[str] = []
        for i in range(0, len(select_field_sqls)):
            if i == 0:
                sql_line = f"SELECT {select_field_sqls[0]}"
            else:
                sql_line = f"       {select_field_sqls[i]}"
            # Append comma all lines except the last one
            if i < len(select_field_sqls) - 1:
                sql_line += ","
            select_sql_lines.append(sql_line)

        return select_sql_lines

    def _build_cte(self, cte: CTE) -> str:
        if isinstance(cte.cte_query, list):
            sql_str: str = self.build_select_sql(cte.cte_query)
        elif isinstance(cte.cte_query, VALUES):
            sql_str: str = self.build_cte_values_sql(values=cte.cte_query, alias_columns=cte.alias_columns)
        elif isinstance(cte.cte_query, str):
            sql_str: str = indent(cte.cte_query, "  ").strip()
        else:
            raise ValueError(f"Unexpected cte_query type: {cte.cte_query.__class__.__name__}")
        sql_str = sql_str.rstrip(";")
        # Do not indent the query string! This could cause issues with multi-line text fields. sql_str = indent(sql_str, "  ")
        alias_columns_str: str = ""
        if cte.alias_columns and self.supports_cte_alias_columns():
            alias_columns_str = "(" + ", ".join([self._build_column_sql(column) for column in cte.alias_columns]) + ")"
        return f"{self.quote_default(cte.alias)}{alias_columns_str} AS (\n{sql_str}\n)"

    def supports_cte_alias_columns(self) -> bool:
        return True

    def _build_cte_sql_lines(self, select_elements: list) -> list[str]:
        cte_lines: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, WITH):
                cte_sql: list[str] = [self._build_cte(cte) for cte in select_element.cte_list]
                cte_sql = "WITH \n" + ",\n".join(cte_sql)
                cte_lines = cte_sql.split("\n")
        return cte_lines

    def build_expression_sql(self, expression: SqlExpression | str | Number) -> str:
        if isinstance(expression, str):
            return self.quote_default(expression)
        elif isinstance(expression, Number):
            return str(expression)
        elif isinstance(expression, COLUMN):
            return self._build_column_sql(expression)
        elif isinstance(expression, LITERAL):
            return self.literal(expression.value)
        elif isinstance(expression, OR):
            return self._build_or_sql(expression)
        elif isinstance(expression, AND):
            return self._build_and_sql(expression)
        elif isinstance(expression, NOT):
            return self._build_not_sql(expression)
        elif isinstance(expression, Operator):
            return self._build_operator_sql(expression)
        elif isinstance(expression, COUNT):
            return self._build_count_sql(expression)
        elif isinstance(expression, SUM):
            return self._build_sum_sql(expression)
        elif isinstance(expression, CASE_WHEN):
            return self._build_case_when_sql(expression)
        elif isinstance(expression, TUPLE):
            return self._build_tuple_sql(expression)
        elif isinstance(expression, COMBINED_HASH):
            return self._build_combined_hash_sql(expression)
        elif isinstance(expression, CONCAT):
            return self._build_concat_sql(expression)
        elif isinstance(expression, CONCAT_WS):
            return self._build_concat_ws_sql(expression)
        elif isinstance(expression, STRING_HASH):
            return self._build_string_hash_sql(expression)
        elif isinstance(expression, IS_NULL):
            return self._build_is_null_sql(expression)
        elif isinstance(expression, IS_NOT_NULL):
            return self._build_is_not_null_sql(expression)
        elif isinstance(expression, REGEX_LIKE):
            return self._build_regex_like_sql(expression)
        elif isinstance(expression, LIKE):
            return self._build_like_sql(expression)
        elif isinstance(expression, IN):
            return self._build_in_sql(expression)
        elif isinstance(expression, IN_SELECT):
            return self._build_in_select_sql(expression)
        elif isinstance(expression, NOT_LIKE):
            return self._build_not_like_sql(expression)
        elif isinstance(expression, LOWER):
            return self._build_lower_sql(expression)
        elif isinstance(expression, LENGTH):
            return self._build_length_sql(expression)
        elif isinstance(expression, MAX):
            return self._build_max_sql(expression)
        elif isinstance(expression, COALESCE):
            return self._build_coalesce_sql(expression)
        elif isinstance(expression, CAST):
            return self._build_cast_sql(expression)
        elif isinstance(expression, FUNCTION):
            return self._build_function_sql(expression)
        elif isinstance(expression, DISTINCT):
            return self._build_distinct_sql(expression)
        elif isinstance(expression, SqlExpressionStr):
            return f"({expression.expression_str})"
        elif isinstance(expression, ORDINAL_POSITION):
            return self._build_ordinal_position_sql(expression)
        elif isinstance(expression, STAR):
            return self._build_star_sql(expression)
        elif isinstance(expression, EXISTS):
            return self._build_exists_sql(expression)
        raise Exception(f"Invalid expression type {expression.__class__.__name__}")

    def _build_column_sql(self, column: COLUMN) -> str:
        table_alias_sql: str = f"{self.quote_default(column.table_alias)}." if column.table_alias else ""
        column_sql: str = self.build_expression_sql(
            column.name
        )  # If column.name is a SqlExpression, it will be compiled; if a string, it will be quoted
        field_alias_sql: str = f" AS {self.quote_default(column.field_alias)}" if column.field_alias else ""
        return f"{table_alias_sql}{column_sql}{field_alias_sql}"

    def _build_or_sql(self, or_expr: OR) -> str:
        if isinstance(or_expr.clauses, list) and len(or_expr.clauses) == 1:
            return self.build_expression_sql(or_expr.clauses[0])
        if isinstance(or_expr.clauses, str) or isinstance(or_expr.clauses, SqlExpression):
            return self.build_expression_sql(or_expr.clauses)
        or_clauses_sql: str = " OR ".join(self.build_expression_sql(or_clause) for or_clause in or_expr.clauses)
        return f"({or_clauses_sql})"

    def _build_not_sql(self, not_expr: NOT) -> str:
        expr_sql: str = self.build_expression_sql(not_expr.expression)
        return f"NOT({expr_sql})"

    def _build_and_sql(self, and_expr: AND) -> str:
        if isinstance(and_expr.clauses, list) and len(and_expr.clauses) == 1:
            return self.build_expression_sql(and_expr.clauses[0])
        if isinstance(and_expr.clauses, str) or isinstance(and_expr.clauses, SqlExpression):
            return self.build_expression_sql(and_expr.clauses)
        return " AND ".join(self.build_expression_sql(and_clause) for and_clause in and_expr.clauses)

    def _build_from_sql_lines(self, select_elements: list) -> list[str]:
        sql_lines: list[str] = []
        # This method formats with newlines and indentation.
        # Alternatively, concatenate all the fields on one line to reduce SQL statement length
        # return "SELECT " + (", ".join(select_fields_sql))
        # For now, we opt for SELECT statement readability...

        from_elements: list[FROM] = [
            select_element for select_element in select_elements if isinstance(select_element, FROM)
        ]

        from_sql_line: str = "FROM "
        for from_element in from_elements:
            if type(from_element) == FROM:
                if from_element is not from_elements[0]:
                    sql_lines.append(f"{from_sql_line},")
                    from_sql_line = "     "
                from_sql_line += self._build_from_part(from_element)
            elif isinstance(from_element, LEFT_INNER_JOIN) or isinstance(from_element, JOIN):
                sql_lines.append(from_sql_line)
                from_sql_line = f"     {self._build_join_part(from_element)}"

        sql_lines.append(from_sql_line)
        return sql_lines

    def _alias_format(self, alias: str) -> str:
        return f"AS {self.quote_default(alias)}"

    def _build_from_part(self, from_part: FROM) -> str:
        # "fully".qualified"."tablename" [AS "table_alias"]

        from_parts: list[str] = [
            self._build_qualified_quoted_dataset_name(
                dataset_name=from_part.table_name, dataset_prefix=from_part.table_prefix
            )
        ]

        if isinstance(from_part.sampler_type, str) and isinstance(from_part.sample_size, Number):
            from_parts.append(self._build_sample_sql(from_part.sampler_type, from_part.sample_size))

        if isinstance(from_part.alias, str):
            from_parts.append(self._alias_format(from_part.alias))

        return " ".join(from_parts)

    def _build_join_part(self, join: LEFT_INNER_JOIN | JOIN) -> str:
        # [INNER JOIN] "fully".qualified"."tablename" [AS "table_alias"] [ON join_condition]

        from_parts: list[str] = []

        if isinstance(join, LEFT_INNER_JOIN):
            from_parts.append("LEFT JOIN")
        if isinstance(join, JOIN):
            from_parts.append("JOIN")

        from_parts.append(
            self._build_qualified_quoted_dataset_name(dataset_name=join.table_name, dataset_prefix=join.table_prefix)
        )

        if isinstance(join.alias, str):
            from_parts.append(self._alias_format(join.alias))

        if isinstance(join, LEFT_INNER_JOIN) or isinstance(join, JOIN):
            from_parts.append(f"ON {self.build_expression_sql(join.on_condition)}")

        return " ".join(from_parts)

    def _build_qualified_quoted_dataset_name(self, dataset_name: str, dataset_prefix: Optional[list[str]]) -> str:
        name_parts: list[str] = [] if dataset_prefix is None else list(dataset_prefix)
        name_parts.append(dataset_name)
        quoted_name_parts: list[str] = [
            self.quote_default(name_part) for name_part in name_parts if isinstance(name_part, str)
        ]
        return ".".join(quoted_name_parts)

    def _build_operator_sql(self, operator: Operator) -> str:
        operators: dict[type, str] = {
            EQ: "=",
            NEQ: "!=",
            LT: "<",
            LTE: "<=",
            GT: ">",
            GTE: ">=",
            LIKE: "like",
        }
        operator_sql: str = operators[type(operator)]
        return f"{self.build_expression_sql(operator.left)} {operator_sql} {self.build_expression_sql(operator.right)}"

    def _build_where_sql_lines(self, select_elements: list) -> list[str]:
        and_expressions: list[SqlExpression] = []
        for select_element in select_elements:
            if isinstance(select_element, WHERE):
                and_expressions.append(select_element.condition)
            elif isinstance(select_element, AND):
                and_expressions.extend(select_element._get_clauses_as_list())

        where_parts: list[str] = [self.build_expression_sql(and_expression) for and_expression in and_expressions]

        where_sql_lines: list[str] = []
        for i in range(0, len(where_parts)):
            if i == 0:
                sql_line = f"WHERE {where_parts[0]}"
            else:
                sql_line = f"  AND {where_parts[i]}"
            where_sql_lines.append(sql_line)
        return where_sql_lines

    def _build_group_by_sql_lines(self, select_elements: list) -> list[str]:
        group_by_field_sqls: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, GROUP_BY):
                if isinstance(select_element.fields, str) or isinstance(select_element.fields, SqlExpression):
                    select_element.fields = [select_element.fields]
                group_by_field_sqls.extend(
                    [self.build_expression_sql(select_field) for select_field in select_element.fields]
                )
        sql_lines: list[str] = []
        if group_by_field_sqls:
            group_by_fields_str: str = ", ".join(group_by_field_sqls)
            sql_lines.append(f"GROUP BY {group_by_fields_str}")
        return sql_lines

    def _build_function_sql(self, function: FUNCTION) -> str:
        args: list[SqlExpression | str] = [function.args] if not isinstance(function.args, list) else function.args
        args_sqls: list[str] = [self.build_expression_sql(arg) for arg in args]
        if function.name in ["+", "-", "/", "*"]:
            operators: str = f" {function.name} ".join(args_sqls)
            return f"({operators})"
        else:
            args_list_sql: str = ", ".join(args_sqls)
            return f"{function.name}({args_list_sql})"

    def _build_star_sql(self, star: STAR) -> str:
        if star.alias:
            return f"{self.quote_default(star.alias)}.*"
        else:
            return "*"

    def _build_count_sql(self, count: COUNT) -> str:
        count_sql = f"COUNT({self.build_expression_sql(count.expression)})"
        if count.field_alias:
            count_sql = f"{count_sql} AS {self.quote_default(count.field_alias)}"
        return count_sql

    def _build_distinct_sql(self, distinct: DISTINCT) -> str:
        expressions: list[SqlExpression] = (
            distinct.expression if isinstance(distinct.expression, list) else [distinct.expression]
        )
        field_expression_str = ", ".join([self.build_expression_sql(e) for e in expressions])
        return f"DISTINCT({field_expression_str})"

    def _build_sum_sql(self, sum: SUM) -> str:
        return f"SUM({self.build_expression_sql(sum.expression)})"

    def _build_is_null_sql(self, is_null: IS_NULL) -> str:
        return f"{self.build_expression_sql(is_null.expression)} IS NULL"

    def _build_is_not_null_sql(self, is_null: IS_NOT_NULL) -> str:
        return f"{self.build_expression_sql(is_null.expression)} IS NOT NULL"

    def _build_in_sql(self, in_: IN) -> str:
        list_expressions: str = ", ".join([self.build_expression_sql(element) for element in in_.list_expression])
        return f"{self.build_expression_sql(in_.expression)} IN ({list_expressions})"

    def _build_in_select_sql(self, in_select: IN_SELECT) -> str:
        nested_select: str = self.build_select_sql(
            select_elements=in_select.nested_select_elements, add_semicolon=False
        )
        nested_select: str = indent(nested_select, "    ")
        return f"{self.build_expression_sql(in_select.expression)} IN (\n{nested_select})"

    def _build_like_sql(self, like: LIKE) -> str:
        return f"{self.build_expression_sql(like.left)} LIKE {self.build_expression_sql(like.right)}"

    def _build_exists_sql(self, exists: EXISTS) -> str:
        nested_select: str = self.build_select_sql(select_elements=exists.nested_select_elements, add_semicolon=False)
        nested_select: str = indent(nested_select, "    ")
        return f"EXISTS (\n{nested_select})"

    def _build_not_like_sql(self, not_like: NOT_LIKE) -> str:
        return f"{self.build_expression_sql(not_like.left)} NOT LIKE {self.build_expression_sql(not_like.right)}"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_LIKE({expression}, '{matches.regex_pattern}')"

    def _build_lower_sql(self, lower: LOWER) -> str:
        return f"LOWER({self.build_expression_sql(lower.expression)})"

    def _build_length_sql(self, length: LENGTH) -> str:
        return f"LENGTH({self.build_expression_sql(length.expression)})"

    def _build_max_sql(self, max: MAX) -> str:
        return f"MAX({self.build_expression_sql(max.expression)})"

    def _build_coalesce_sql(self, coalesce: COALESCE) -> str:
        args: str = ", ".join([self.build_expression_sql(expression) for expression in coalesce.args])
        return f"COALESCE({args})"

    def _build_cast_sql(self, cast: CAST) -> str:
        to_type_text: str = (
            self.get_data_source_data_type_name_for_soda_data_type_name(cast.to_type)
            if isinstance(cast.to_type, SodaDataTypeName)
            else cast.to_type
        )
        return f"CAST({self.build_expression_sql(cast.expression)} AS {to_type_text})"

    def _build_case_when_sql(self, case_when: CASE_WHEN) -> str:
        return (
            f"CASE WHEN {self.build_expression_sql(case_when.condition)} "
            + f"THEN {self.build_expression_sql(case_when.if_expression)} "
            + (f"ELSE {self.build_expression_sql(case_when.else_expression)} " if case_when.else_expression else "")
            + "END"
        )

    def _build_order_by_lines(self, select_elements: list) -> list[str]:
        order_by_clauses: list[str] = []
        for select_element in select_elements:
            if isinstance(select_element, ORDER_BY_ASC) or isinstance(select_element, ORDER_BY_DESC):
                expression = select_element.expression
                direction: str = " ASC" if isinstance(select_element, ORDER_BY_ASC) else " DESC"
                order_by_clauses.append(f"{self.build_expression_sql(expression)}{direction}")
        if order_by_clauses:
            order_by_text: str = ", ".join(order_by_clauses)
            return [f"ORDER BY {order_by_text}"]
        else:
            return []

    def _build_limit_line(self, select_elements: list) -> Optional[str]:
        for select_element in select_elements:
            if isinstance(select_element, LIMIT):
                return self._build_limit_sql(select_element)

        return None

    def _build_offset_line(self, select_elements: list) -> Optional[str]:
        for select_element in select_elements:
            if isinstance(select_element, OFFSET):
                return self._build_offset_sql(select_element)

        return None

    def _build_ordinal_position_sql(self, ordinal_position: ORDINAL_POSITION) -> str:
        return "ORDINAL_POSITION"

    def _build_limit_sql(self, limit_element: LIMIT) -> str:
        return f"LIMIT {limit_element.limit}"

    def _build_offset_sql(self, offset_element: OFFSET) -> str:
        return f"OFFSET {offset_element.offset}"

    def supports_function(self, function: str) -> bool:
        return function in ["avg", "avg_length", "max", "min", "max_length", "min_length", "sum"]

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        elements: str = ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
        return f"({elements})"

    def get_soda_null_string_value(self) -> str:
        return "__SODA_NULL__"

    def _build_concat_sql(self, concat: CONCAT) -> str:
        elements: str = ", ".join(self.build_expression_sql(e) for e in concat.expressions)
        return f"CONCAT({elements})"

    def _build_concat_ws_sql(self, concat_ws: CONCAT_WS) -> str:
        elements: str = ", ".join(self.build_expression_sql(e) for e in concat_ws.expressions)
        return f"CONCAT_WS({concat_ws.separator}, {elements})"

    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        return f"MD5({self.build_expression_sql(string_hash.expression)})"

    def _build_combined_hash_sql(self, combined_hash: COMBINED_HASH) -> str:
        """Convert a set of columns into a unique hashed string which can be used as a key."""

        def format_expr(e: SqlExpression) -> SqlExpression:
            """Convert expression to a string, and replace nulls with a predefined string."""
            return COALESCE([CAST(e, to_type=SodaDataTypeName.VARCHAR), LITERAL(self.get_soda_null_string_value())])

        formatted_expressions: list[SqlExpression] = [format_expr(e) for e in combined_hash.expressions]
        if len(formatted_expressions) == 1:
            string_to_hash = formatted_expressions[0]
        else:
            string_to_hash = CONCAT_WS(separator="'||'", expressions=formatted_expressions)
        return self.build_expression_sql(STRING_HASH(string_to_hash))

    def _build_sample_sql(self, sampler_type: str, sample_size: Number) -> str:
        raise NotImplementedError("Sampling not implemented for this dialect")

    def information_schema_namespace_elements(self, data_source_namespace: DataSourceNamespace) -> list[str]:
        """
        The prefixes / namespace of the information schema for a given dataset prefix / namespace
        """
        database_name: str | None = data_source_namespace.get_database_for_metadata_query()
        if database_name:
            return [database_name, self.schema_information_schema()]
        else:
            return [self.schema_information_schema()]

    def schema_information_schema(self) -> str | None:
        """
        Name of the schema that has the metadata
        """
        return self.default_casify("information_schema")

    def table_tables(self) -> str:
        """
        Name of the table that has the table information in the metadata
        """
        return self.default_casify("tables")

    def table_columns(self) -> str:
        """
        Name of the table that has the columns information in the metadata.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("columns")

    def table_schemata(self) -> str:
        """
        Name of the table that has the schema information in the metadata.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("schemata")

    def column_schemata_catalog_name(self) -> str:
        """
        Name of the column that has the database/catalog information in the schemata metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("catalog_name")

    def column_schema_name(self) -> str:
        """
        Name of the column that has the schema name in the schemata metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("schema_name")

    def column_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table
        """
        return self.default_casify("table_catalog")

    def column_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table
        """
        return self.default_casify("table_schema")

    def column_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return self.default_casify("table_name")

    def column_column_name(self) -> str:
        """
        Name of the column that has the column name in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("column_name")

    def column_data_type(self) -> str:
        """
        Name of the column that has the data type in the columns metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("data_type")

    def column_data_type_max_length(self) -> Optional[str]:
        """
        Name or definition of the column that has the max data type length in the columns metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("character_maximum_length")

    def supports_data_type_character_maximum_length(self) -> bool:
        return True

    def column_data_type_numeric_precision(self) -> Optional[str]:
        """
        Name or definition of the column that has the max data type length in the columns metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("numeric_precision")

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def column_data_type_numeric_scale(self) -> Optional[str]:
        """
        Name or definition of the column that has the max data type length in the columns metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("numeric_scale")

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def column_data_type_datetime_precision(self) -> Optional[str]:
        """
        Name or definition of the column that has the max data type length in the columns metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return self.default_casify("datetime_precision")

    def supports_data_type_datetime_precision(self) -> bool:
        return True

    def supports_datetime_microseconds(self) -> bool:
        return True

    def default_casify(self, identifier: str) -> str:
        return identifier

    def metadata_casify(self, identifier: str) -> str:
        """Define case for metadata identifiers if needed."""
        return identifier

    def create_table_casify_qualified_name(self, qualified_name: str) -> str:
        # For data sources that default to a specific case for the table name (e.g. Snowflake prefers uppercase), this method can be overridden to casify the qualified table name.
        return qualified_name  # By default, we don't casify the qualified table name when creating a table as these are quoted

    # Very lightweight dialect-specific interpretation of dataset prefixes.
    def get_database_prefix_index(self) -> int | None:
        return 0

    # Very lightweight dialect-specific interpretation of dataset prefixes.
    def get_schema_prefix_index(self) -> int | None:
        return 1

    def sql_expr_timestamp_with_tz_literal(self, datetime_in_iso8601: str) -> str:
        """Convert to a SQL representation of a timestamp with timezone.

        By default this will return the standard SQL timestamp representation but may be overridden.
        We may wish to add some logic to detect timezones in datetime and return the appropriate representation.
        For now it's up to the user to decide which representation to use.
        """
        return self.sql_expr_timestamp_literal(datetime_in_iso8601)

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp '{datetime_in_iso8601}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc('day', {timestamp_literal})"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval '1 day'"

    def quote_column(self, column_name: str) -> str:
        return self.quote_default(column_name)

    def format_metadata_data_type(self, data_type: str) -> str:
        """Allows processing data type string result from metadata column query if needed (Oracle uses this)."""
        return data_type

    def supports_regex_advanced(self) -> bool:
        return True  # Default to true, but specific dialects can override to false

    def encode_string_for_sql(self, string: str) -> str:
        """This escapes values that contain newlines correctly."""
        return string.encode("unicode_escape").decode("utf-8")

    def get_max_table_name_length(self) -> int:
        return 63

    def get_max_sql_statement_length(self) -> int:
        # What is the maximum query length of common analytical databases?
        # ChatGPT said:
        # Here are the maximum query lengths for some common analytical databases:
        # PostgreSQL: 1 GB
        # MySQL: 1 MB (configurable via max_allowed_packet)
        # SQL Server: 65,536 bytes (approximately 65 KB)
        # Oracle: 64 KB (depends on SQL string encoding)
        # Snowflake: 1 MB
        # BigQuery: No documented limit on query size, but practical limits on complexity and performance.
        return 63 * 1024 * 1024

    def get_preferred_number_of_rows_for_insert(self) -> int:
        # 10000 is the default number of rows for insert in a single SQL statement that is sent to the data source.
        # Please overwrite this for each data source as needed.
        # Currently, we set this value relateively low so the memory consumption of the Python library is not too high.
        return 10000

    def is_quoted(self, identifier: str) -> bool:
        return identifier.startswith(self.DEFAULT_QUOTE_CHAR) and identifier.endswith(self.DEFAULT_QUOTE_CHAR)

    def default_numeric_precision(self) -> Optional[int]:
        return None

    def default_numeric_scale(self) -> Optional[int]:
        return None

    def get_sql_data_type_class(self) -> type:
        return SqlDataType

    def supports_case_sensitive_column_names(self) -> bool:
        return True

    def apply_sampling(
        self,
        sql: str,
        sampler_limit: Number,
        sampler_type: SamplerType,
    ) -> str:
        return apply_sampling_to_sql(
            sql=sql,
            sampler_limit=sampler_limit,
            sampler_type=sampler_type,
            read_dialect=self.data_source_impl.type_name,
            write_dialect=self.data_source_impl.type_name,
        )

    ########################################################
    # Metadata columns query
    ########################################################

    def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """

        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()

        information_schema_namespace_elements = self.information_schema_namespace_elements(table_namespace)

        return self.build_select_sql(
            [
                SELECT(
                    [
                        self.column_column_name(),
                        self.column_data_type(),
                        *(
                            [self.column_data_type_max_length()]
                            if self.supports_data_type_character_maximum_length() and self.column_data_type_max_length()
                            else []
                        ),
                        *(
                            [self.column_data_type_numeric_precision()]
                            if self.supports_data_type_numeric_precision() and self.column_data_type_numeric_precision()
                            else []
                        ),
                        *(
                            [self.column_data_type_numeric_scale()]
                            if self.supports_data_type_numeric_scale() and self.column_data_type_numeric_scale()
                            else []
                        ),
                        *(
                            [self.column_data_type_datetime_precision()]
                            if self.supports_data_type_datetime_precision()
                            and self.column_data_type_datetime_precision()
                            else []
                        ),
                    ]
                ),
                FROM(self.table_columns()).IN(information_schema_namespace_elements),
                WHERE(
                    AND(
                        [
                            *(
                                [EQ(self.column_table_catalog(), LITERAL(self.metadata_casify(database_name)))]
                                if database_name
                                else []
                            ),
                            EQ(self.column_table_schema(), LITERAL(self.metadata_casify(schema_name))),
                            EQ(self.column_table_name(), LITERAL(self.metadata_casify(table_name))),
                        ]
                    )
                ),
                ORDER_BY_ASC(ORDINAL_POSITION()),
            ]
        )

    def build_schemas_metadata_query_str(
        self, table_namespace: Optional[DataSourceNamespace] = None, filter_on_schema_name: Optional[str] = None
    ) -> str:
        """
        Builds the full SQL query to query schema names from the data source metadata.
        Optionally filters by database/catalog if provided in table_namespace.

        If the filter_on_schema_name is provided, it will only return the schema name that matches the filter. This is useful to check if a schema exists.
        """
        database_name: str | None = table_namespace.get_database_for_metadata_query() if table_namespace else None

        information_schema_namespace_elements = self.information_schema_namespace_elements(table_namespace)

        select_elements = [
            SELECT([self.column_schema_name()]),
            FROM(self.table_schemata()).IN(information_schema_namespace_elements),
        ]

        and_elements = [
            EQ(self.column_schemata_catalog_name(), LITERAL(self.metadata_casify(database_name))),
        ]

        if filter_on_schema_name:
            and_elements.append(EQ(self.column_schema_name(), LITERAL(self.metadata_casify(filter_on_schema_name))))

        if database_name:
            select_elements.append(WHERE(AND(and_elements)))

        return self.build_select_sql(select_elements)

    def extract_column_index(self, column_name: str, columns: list[Tuple[Any, ...]]) -> int:
        column_names = [c[0] for c in columns]
        return column_names.index(column_name)

    def extract_data_type_name(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> str:
        return row[1]

    def extract_character_maximum_length(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        """Extract character maximum length from column metadata.  Typically this is just the value of a specific column."""
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_character_maximum_length(data_type_name):
            return None
        if self.supports_data_type_character_maximum_length() and self.column_data_type_max_length():
            col_index = self.extract_column_index(self.column_data_type_max_length(), columns)
            return row[col_index]
        return None

    def extract_numeric_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        """Extract numeric precision from column metadata.  Typically this is just the value of a specific column."""
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_precision(data_type_name):
            return None

        if self.supports_data_type_numeric_precision() and self.column_data_type_numeric_precision():
            col_index = self.extract_column_index(self.column_data_type_numeric_precision(), columns)
            return row[col_index]
        return None

    def extract_numeric_scale(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        """Extract numeric scale from column metadata.  Typically this is just the value of a specific column."""
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_scale(data_type_name):
            return None

        if self.supports_data_type_numeric_scale() and self.column_data_type_numeric_scale():
            col_index = self.extract_column_index(self.column_data_type_numeric_scale(), columns)
            return row[col_index]
        return None

    def extract_datetime_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        """Extract datetime precision from column metadata.  Typically this is just the value of a specific column."""
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_datetime_precision(data_type_name):
            return None

        if self.supports_data_type_datetime_precision() and self.column_data_type_datetime_precision():
            col_index = self.extract_column_index(self.column_data_type_datetime_precision(), columns)
            return row[col_index]
        return None

    def build_column_metadatas_from_query_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        column_metadatas: list[ColumnMetadata] = []
        for row in query_result.rows:
            column_name: str = row[0]
            data_type_name: str = self.format_metadata_data_type(self.extract_data_type_name(row, query_result.columns))
            character_maximum_length: Optional[int] = self.extract_character_maximum_length(row, query_result.columns)
            numeric_precision: Optional[int] = self.extract_numeric_precision(row, query_result.columns)
            numeric_scale: Optional[int] = self.extract_numeric_scale(row, query_result.columns)
            datetime_precision: Optional[int] = self.extract_datetime_precision(row, query_result.columns)

            column_metadatas.append(
                ColumnMetadata(
                    column_name=column_name,
                    sql_data_type=self.get_sql_data_type_class()(
                        name=data_type_name,
                        character_maximum_length=character_maximum_length,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        datetime_precision=datetime_precision,
                    ),
                )
            )
        return column_metadatas

    def get_column_name(self, column: Any) -> str:
        """Extract the column name from an element in cursor.description"""
        if type(column) == tuple:
            return column[0]
        try:
            return column.name
        except:
            raise NotImplementedError(f"Unknown column type: {type(column)}")

    def get_column_index(self, column_name: str, columns: list[Any]) -> int:
        """Extract the column index from an element in cursor.description"""
        col_names = [self.get_column_name(c) for c in columns]
        return col_names.index(column_name)
