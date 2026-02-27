import logging
import re
from datetime import datetime, time
from typing import Any, Optional, Tuple

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import INSERT_INTO_VIA_SELECT, STRING_HASH
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSource as TrinoDataSourceModel,
)
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSourceConnection,
)
from soda_trino.statements.trino_metadata_tables_query import TrinoMetadataTablesQuery

logger: logging.Logger = soda_logger


class TrinoDataSourceImpl(DataSourceImpl, model_class=TrinoDataSourceModel):
    def __init__(
        self,
        data_source_model: TrinoDataSourceModel,
        connection: Optional[DataSourceConnection] = None,
    ):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return TrinoSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return TrinoDataSourceConnection(
            name=self.data_source_model.name,
            connection_properties=self.data_source_model.connection_properties,
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return TrinoMetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
        )


class TrinoSqlDataType(SqlDataType):
    def get_sql_data_type_str_with_parameters(self) -> str:
        if isinstance(self.datetime_precision, int) and self.name == "timestamp":
            return f"timestamp({self.datetime_precision})"
        if isinstance(self.datetime_precision, int) and self.name == "timestamp with time zone":
            return f"timestamp({self.datetime_precision}) with time zone"
        return super().get_sql_data_type_str_with_parameters()


class TrinoSqlDialect(SqlDialect, sqlglot_dialect="trino"):
    USES_SEMICOLONS_BY_DEFAULT: bool = False
    SUPPORTS_DROP_TABLE_CASCADE: bool = False

    # Trino connectors may promote types (e.g. Iceberg: char→varchar, smallint→integer).
    # These synonyms prevent false schema check failures across all connectors.
    # NOTE: These are bidirectional — a contract specifying INTEGER will also pass when the
    # actual column is SMALLINT (and vice versa). Ideally promotion would be directional
    # (smallint→integer OK, integer→smallint fail), but the synonym system doesn't support
    # that yet. Acceptable for now since the reverse mismatch is rare in practice.
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.SMALLINT, SodaDataTypeName.INTEGER),
    )

    def supports_materialized_views(self) -> bool:
        # TODO  this should inherit from the connection
        return True

    def supports_views(self) -> bool:
        # TODO  this should inherit from the connection
        return True

    def supports_case_sensitive_column_names(self) -> bool:
        # TODO -- it's possible that some connectors do support case sensitive names, investigate
        # This open issue seems to suggest that support is still pending https://github.com/trinodb/trino/issues/17
        return False

    def is_same_data_type_for_schema_check(self, expected: SqlDataType, actual: SqlDataType):
        # Trino connectors vary in type parameter fidelity — e.g. Iceberg drops varchar
        # lengths and normalizes timestamp precision to 6. Only compare parameters when
        # both sides report a value (a None on the actual side means the connector doesn't
        # track that parameter), and skip datetime precision entirely since connectors
        # normalize it to connector-specific defaults (postgres→3, iceberg→6).
        if not self.data_type_names_are_same_or_synonym(expected.name, actual.name):
            return False
        if (
            isinstance(expected.character_maximum_length, int)
            and isinstance(actual.character_maximum_length, int)
            and expected.character_maximum_length != actual.character_maximum_length
        ):
            return False
        if (
            isinstance(expected.numeric_precision, int)
            and isinstance(actual.numeric_precision, int)
            and expected.numeric_precision != actual.numeric_precision
        ):
            return False
        if (
            isinstance(expected.numeric_scale, int)
            and isinstance(actual.numeric_scale, int)
            and expected.numeric_scale != actual.numeric_scale
        ):
            return False
        # datetime_precision intentionally not compared — Trino connectors normalize
        # it to connector-specific defaults (postgres→3, iceberg→6).
        return True

    def get_sql_data_type_class(self) -> type:
        return TrinoSqlDataType

    @staticmethod
    def _extract_type_parameter(raw: str) -> Optional[int]:
        """Extract the integer parameter from a raw type string like 'varchar(255)' or 'timestamp(3)'."""
        match = re.search(r"\((\d+)\)", raw)
        return int(match.group(1)) if match else None

    def extract_data_type_name(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> str:
        raw = row[1]
        # "timestamp(3) with time zone" → "timestamp with time zone"
        return re.sub(r"\(\d+\)", "", raw).replace("  ", " ").strip()

    def extract_character_maximum_length(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_character_maximum_length(data_type_name):
            return None
        return self._extract_type_parameter(row[1])

    def extract_numeric_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_precision(data_type_name):
            return None
        formatted_data_type_name: str = self.format_metadata_data_type(data_type_name)
        data_type_tuple = data_type_name[len(formatted_data_type_name) + 1 : -1].split(",")
        return int(data_type_tuple[0])

    def extract_numeric_scale(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_scale(data_type_name):
            return None

        formatted_data_type_name: str = self.format_metadata_data_type(data_type_name)
        data_type_tuple = data_type_name[len(formatted_data_type_name) + 1 : -1].split(",")
        return int(data_type_tuple[1])

    def extract_datetime_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_datetime_precision(data_type_name):
            return None
        return self._extract_type_parameter(row[1])

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        # Trino connectors may promote types (e.g. Iceberg: char→varchar, smallint→integer).
        # These synonyms prevent false schema check failures across all connectors.
        return [
            ["SMALLINT", "INTEGER", "INT"],
            ["CHAR", "VARCHAR"],
        ]

    def get_data_source_data_type_name_by_soda_data_type_names(
        self,
    ) -> dict[SodaDataTypeName, str]:
        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "varchar",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.FLOAT: "real",
            SodaDataTypeName.DOUBLE: "double",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp with time zone",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(
        self,
    ) -> dict[str, SodaDataTypeName]:
        return {
            "boolean": SodaDataTypeName.BOOLEAN,
            "tinyint": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "real": SodaDataTypeName.FLOAT,
            "double": SodaDataTypeName.DOUBLE,
            "decimal": SodaDataTypeName.DECIMAL,
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
        }

    def column_data_type_max_length(self) -> Optional[str]:
        return None

    def supports_data_type_character_maximum_length(self) -> bool:
        return True

    def column_data_type_numeric_precision(self) -> Optional[str]:
        return None

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def column_data_type_numeric_scale(self) -> Optional[str]:
        return None

    def default_numeric_precision(self) -> Optional[int]:
        return 38

    def default_numeric_scale(self) -> Optional[int]:
        return 0

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def column_data_type_datetime_precision(self) -> Optional[str]:
        return None

    def supports_data_type_datetime_precision(self) -> bool:
        return True

    def supports_datetime_microseconds(self) -> bool:
        return True

    def get_max_sql_statement_length(self) -> int:
        # Trino's default query.max-length is 1,000,000 bytes (not 1 MiB).
        # Use a conservative limit to stay safely under the server default.
        return 1_000_000

    def metadata_casify(self, identifier: str) -> str:
        # trino lower-cases metadata identifiers
        return identifier.lower()

    def format_metadata_data_type(self, data_type: str) -> str:
        """Strip modifiers"""
        parenthesis_index = data_type.find("(")
        if parenthesis_index != -1:
            return data_type[:parenthesis_index]
        return data_type

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "varchar",
            "char",
        ]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() == "decimal"

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() == "decimal"

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "timestamp",
            "timestamp with time zone",
            "time",
            "time with time zone",  # unsupported in Soda for now
        ]

    def literal_datetime(self, datetime: datetime):
        return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    def literal_datetime_with_tz(self, datetime: datetime):
        return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"

    def literal_time(self, time: time):
        formatted_time = time.strftime("%H:%M:%S.%f")
        return f"TIME '{formatted_time}'"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        formatted_datetime = datetime_in_iso8601.replace("T", " ")
        return f"TIMESTAMP '{formatted_datetime}'"

    def convert_str_to_datetime(self, datetime_str: str) -> datetime:
        datetime_iso_str = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S.%f%z").isoformat()
        return convert_str_to_datetime(datetime_iso_str)

    def convert_datetime_to_str(self, datetime: datetime) -> str:
        datetime_iso_str: str = convert_datetime_to_str(datetime)
        return datetime_iso_str.replace("T", " ")

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"DATE_ADD('day', 1, {timestamp_literal})"

    def build_select_sql(self, select_elements: list, add_semicolon: Optional[bool] = None) -> str:
        # Trino requires OFFSET before LIMIT (standard SQL order).
        # The base implementation emits LIMIT before OFFSET, which is invalid in Trino.
        add_semicolon = self.apply_default_add_semicolon(add_semicolon)
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
            statement_lines.append(limit_line)

        return "\n".join(statement_lines) + (";" if add_semicolon else "")

    def build_insert_into_via_select_sql(
        self, insert_into_via_select: INSERT_INTO_VIA_SELECT, add_semicolon: Optional[bool] = None
    ) -> str:
        # Trino's grammar does not allow a WITH clause inside parentheses in INSERT INTO context.
        # The base implementation wraps the SELECT (including WITH) in parens:
        #   INSERT INTO t (cols) (WITH cte AS (...) SELECT ...)  -- INVALID in Trino
        # We emit it without parens:
        #   INSERT INTO t (cols) WITH cte AS (...) SELECT ...    -- VALID in Trino
        add_semicolon = self.apply_default_add_semicolon(add_semicolon)
        insert_into_sql: str = f"INSERT INTO {insert_into_via_select.fully_qualified_table_name}\n"
        insert_into_sql += self._build_insert_into_columns_sql(insert_into_via_select) + "\n"
        insert_into_sql += self.build_select_sql(insert_into_via_select.select_elements, add_semicolon=False)
        return insert_into_sql + (";" if add_semicolon else "")

    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        # all Trino hash methods operate on binary data - TO_UTF8 converts string to binary
        return f"TO_HEX(MD5(TO_UTF8({self.build_expression_sql(string_hash.expression)})))"
