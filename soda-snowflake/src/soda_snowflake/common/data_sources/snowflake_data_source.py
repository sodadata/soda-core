from logging import Logger
from numbers import Number
from typing import TYPE_CHECKING, Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.exceptions import UnsupportedSqlStatementError
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import ColumnMetadata, DataSourceNamespace, SamplerType, SodaDataTypeName
from soda_core.common.sql_ast import (
    ADD_INTERVAL,
    AND,
    COLUMN,
    COUNT,
    DATE_ISO_TEXT,
    DATE_TRUNC,
    DISTINCT,
    EPOCH_SECONDS,
    EQ,
    FROM,
    IN,
    JSON_MERGE,
    LITERAL,
    RANDOM,
    REGEXP_REPLACE,
    SELECT,
    SOURCE_COLUMN,
    TIME_DELTA,
    TIMESTAMP_ISO_TEXT,
    TO_TIMEZONE,
    TUPLE,
    UPSERT,
    UPSERT_VIA_SELECT,
    VALUES,
    WHERE,
    seconds_per_time_bucket,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_primary_keys_query import MetadataPrimaryKeysQuery
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSource as SnowflakeDataSourceModel,
)
from soda_snowflake.common.data_sources.snowflake_data_source_connection import SnowflakeDataSourceConnection

logger: Logger = soda_logger

if TYPE_CHECKING:
    from soda_core.contracts.impl.contract_verification_impl import ContractImpl

TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone"
TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone"
TIMESTAMP_WITH_LOCAL_TIME_ZONE = "timestamp with local time zone"


class SnowflakeMetadataPrimaryKeysQuery(MetadataPrimaryKeysQuery):
    """Primary-key introspection for Snowflake.

    Snowflake's INFORMATION_SCHEMA has a TABLE_CONSTRAINTS view but NO KEY_COLUMN_USAGE view,
    so the base information_schema JOIN would raise a compilation error. The key columns only
    come from SHOW PRIMARY KEYS — but SHOW caps its output at ~10K rows, so one schema-wide
    SHOW silently drops PKs for tables past the cap in a large schema. Two steps instead:

    1. build_sql_statement queries TABLE_CONSTRAINTS (a real view: filterable, no row cap)
       for which of the requested tables have a PRIMARY KEY constraint.
    2. One ``SHOW PRIMARY KEYS IN TABLE`` per such table — its output is that table's key
       columns only, far below any cap — ordered by key_sequence.
    """

    def execute(self, dataset_prefixes: list[str], dataset_names: list[str]) -> dict[str, list[str]]:
        if not dataset_names:
            return {}
        namespace: DataSourceNamespace = self._build_namespace(dataset_prefixes)
        select_statement: list = self.build_sql_statement(table_namespace=namespace, table_names=dataset_names)
        sql: str = self.sql_dialect.build_select_sql(select_statement)
        # TABLE_CONSTRAINTS returns table names in Snowflake's stored casing; reuse them verbatim
        # to qualify the SHOW statements and key the result.
        tables_with_primary_key: list[str] = [
            row[0] for row in self.data_source_connection.execute_query(sql).rows if row and row[0]
        ]

        database_name: str | None = namespace.get_database_for_metadata_query()
        schema_name: str = namespace.get_schema_for_metadata_query()
        primary_keys_by_table: dict[str, list[str]] = {}
        for table_name in tables_with_primary_key:
            name_parts: list[str] = ([database_name] if database_name else []) + [schema_name, table_name]
            qualified_table: str = ".".join(self.sql_dialect.quote_default(part) for part in name_parts)
            show_result: QueryResult = self.data_source_connection.execute_query(
                f"SHOW PRIMARY KEYS IN TABLE {qualified_table}"
            )
            primary_keys_by_table[table_name] = self._ordered_key_columns(show_result)
        return primary_keys_by_table

    def build_sql_statement(self, table_namespace: DataSourceNamespace, table_names: list[str]) -> list:
        """Selects the requested tables that have a PRIMARY KEY constraint from
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS (which exists on Snowflake; KEY_COLUMN_USAGE does not).
        """
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()
        information_schema = self.sql_dialect.information_schema_namespace_elements(table_namespace)

        return [
            SELECT([COLUMN(self.sql_dialect.column_table_name())]),
            FROM(self.table_table_constraints()).IN(information_schema),
            WHERE(
                AND(
                    [
                        EQ(
                            COLUMN(self.column_constraint_type()),
                            LITERAL(self.primary_key_constraint_type_value()),
                        ),
                        *(
                            [
                                EQ(
                                    COLUMN(self.sql_dialect.column_table_catalog()),
                                    LITERAL(self.sql_dialect.metadata_casify(database_name)),
                                )
                            ]
                            if database_name
                            else []
                        ),
                        EQ(
                            COLUMN(self.sql_dialect.column_table_schema()),
                            LITERAL(self.sql_dialect.metadata_casify(schema_name)),
                        ),
                        IN(
                            COLUMN(self.sql_dialect.column_table_name()),
                            [LITERAL(self.sql_dialect.metadata_casify(name)) for name in table_names],
                        ),
                    ]
                )
            ),
        ]

    @staticmethod
    def _ordered_key_columns(show_result: QueryResult) -> list[str]:
        """Extracts the key column names from one table's SHOW PRIMARY KEYS output, ordered by
        key_sequence (the 1-based position within the key). Columns are located by name in the
        cursor description rather than by position, so a change in SHOW's output order fails
        loud instead of silently mis-parsing."""
        column_names: list[str] = [str(column[0]).lower() for column in show_result.columns]
        column_name_index: int = column_names.index("column_name")
        key_sequence_index: int = column_names.index("key_sequence")
        ordered_columns: list[tuple[int, str]] = sorted(
            (int(row[key_sequence_index]), row[column_name_index])
            for row in show_result.rows
            if row[column_name_index] is not None
        )
        return [column_name for _, column_name in ordered_columns]


class SnowflakeDataSourceImpl(DataSourceImpl, model_class=SnowflakeDataSourceModel):
    def __init__(self, data_source_model: SnowflakeDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return SnowflakeSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SnowflakeDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_primary_keys_query(self) -> MetadataPrimaryKeysQuery:
        # TABLE_CONSTRAINTS + per-table SHOW PRIMARY KEYS: Snowflake has no KEY_COLUMN_USAGE
        # view, and a schema-wide SHOW caps at ~10K rows. See SnowflakeMetadataPrimaryKeysQuery.
        return SnowflakeMetadataPrimaryKeysQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )

    def switch_warehouse(self, warehouse: str, contract_impl: ContractImpl) -> None:
        if warehouse and contract_impl.datasource_warehouse != warehouse:
            if contract_impl.datasource_warehouse is None:
                logger.info(
                    f"Setting warehouse to '{warehouse}' for Contract verification of dataset '{contract_impl.dataset_identifier.to_string()}'"
                )
            else:
                logger.info(
                    f"Switching warehouse from '{contract_impl.datasource_warehouse}' to '{warehouse}' for Contract verification of dataset '{contract_impl.dataset_identifier.to_string()}'"
                )
            self._execute_switch_warehouse(warehouse)
        else:
            logger.info(
                f"Using warehouse '{contract_impl.datasource_warehouse}' for Contract verification of dataset '{contract_impl.dataset_identifier.to_string()}'"
            )

    def _execute_switch_warehouse(self, warehouse: str) -> None:
        switch_warehouse_sql = f"USE WAREHOUSE {warehouse}"
        self.execute_query(switch_warehouse_sql)

    def get_current_warehouse(self) -> Optional[str]:
        sql = "SELECT CURRENT_WAREHOUSE()"
        result = self.execute_query(sql)
        result_rows = result.rows
        row = result_rows[0] if result_rows else None

        return row[0] if row and row[0] else None


class SnowflakeSqlDialect(SqlDialect, sqlglot_dialect="snowflake"):
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (
            SodaDataTypeName.NUMERIC,
            SodaDataTypeName.DECIMAL,
            SodaDataTypeName.INTEGER,
            SodaDataTypeName.BIGINT,
            SodaDataTypeName.SMALLINT,
        ),
        (SodaDataTypeName.FLOAT, SodaDataTypeName.DOUBLE),
    )
    SUPPORTS_DATA_PLANE_STATEMENTS = True

    def escape_string(self, value: str):
        # Snowflake treats backslash as an escape character in SQL string
        # literals (e.g. '\n' → newline).  Double backslashes first so they
        # are interpreted as literal backslashes, then escape single quotes.
        string_literal: str = value.replace("\\", "\\\\")
        string_literal = string_literal.replace("'", "''")
        return string_literal

    def supports_primary_keys(self) -> bool:
        return True

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def metadata_casify(self, identifier: str) -> str:
        """Metadata identifiers are not uppercased for Snowflake."""
        return identifier

    def create_table_casify_qualified_name(self, qualified_name: str) -> str:
        # Parse the last element from the qualified name and casify it
        if "." in qualified_name:  # It is an actual qualified name, not just a table name
            name_parts = qualified_name.split(".")
            # Caseify the last element
            name_parts[-1] = name_parts[-1].upper()
            return ".".join(name_parts)
        # It is not a fully qualified name, just a table name
        return qualified_name.upper()

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return " SELECT * FROM VALUES\n" + ",\n".join([self.build_expression_sql(value) for value in values.values])

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"ARRAY_CONSTRUCT{super()._build_tuple_sql(tuple)}"

    def _build_time_delta_sql(self, time_delta: TIME_DELTA) -> str:
        """Snowflake TIMESTAMPDIFF only counts crossed boundaries of the given
        unit, so compute the difference in SECONDS and divide by the float
        seconds-per-interval."""
        start_sql: str = self.build_expression_sql(time_delta.start)
        end_sql: str = self.build_expression_sql(time_delta.end)
        multiplier: float = seconds_per_time_bucket(time_delta.unit, time_delta.count) / 1.0
        return f"FLOOR(TIMESTAMPDIFF(second, {start_sql}, {end_sql}) / {multiplier})"

    def _build_add_interval_sql(self, add_interval: ADD_INTERVAL) -> str:
        timestamp_sql: str = self.build_expression_sql(add_interval.timestamp)
        count_sql: str = self.build_expression_sql(add_interval.count_expression)
        return f"TIMESTAMPADD({add_interval.unit}, {count_sql}, {timestamp_sql})"

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        # Implements data type synonyms
        # Each list should represent a list of synonyms
        return [
            ["varchar", "text", "string"],
            ["number", "decimal", "numeric", "int", "integer", "bigint", "smallint", "tinyint", "byteint"],
            ["float", "float4", "float8", "double", "double precision", "real"],
            ["timestamp", "datetime", "timestamp_ntz", TIMESTAMP_WITHOUT_TIME_ZONE],
            ["timestamp_ltz", TIMESTAMP_WITH_LOCAL_TIME_ZONE],
            ["timestamp_tz", TIMESTAMP_WITH_TIME_ZONE],
        ]

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        """
        Maps DBDataType names to data source type names.
        """
        return {
            SodaDataTypeName.CHAR: "char",  # Note: by default a char is 1 byte in Snowflake!
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "text",  # alias for varchar
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.DECIMAL: "number",  # decimal & numeric → number
            SodaDataTypeName.NUMERIC: "number",
            SodaDataTypeName.FLOAT: "float",  # float / double → float
            SodaDataTypeName.DOUBLE: "float",
            SodaDataTypeName.TIMESTAMP: "timestamp_ntz",  # default timestamp in snowflake
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp_tz",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", "character", "text"]

    def get_large_numeric_cast_type_name(self) -> Optional[str]:
        """CAST aggregate args to FLOAT so math over NUMBER columns runs in float,
        not scaled NUMBER."""
        return "FLOAT"

    # TODO: test this thorough. The code here is generated using AI just to be able to test the E2E.
    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "character": SodaDataTypeName.CHAR,
            "text": SodaDataTypeName.TEXT,
            "string": SodaDataTypeName.VARCHAR,
            # Defensive aliases — unreachable via INFORMATION_SCHEMA (Snowflake
            # canonicalizes every string type to TEXT).
            "nchar": SodaDataTypeName.CHAR,
            "nvarchar": SodaDataTypeName.VARCHAR,
            "nvarchar2": SodaDataTypeName.VARCHAR,
            "char varying": SodaDataTypeName.VARCHAR,
            "nchar varying": SodaDataTypeName.VARCHAR,
            "smallint": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "int": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "tinyint": SodaDataTypeName.SMALLINT,
            "byteint": SodaDataTypeName.SMALLINT,
            "number": SodaDataTypeName.NUMERIC,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            "float8": SodaDataTypeName.DOUBLE,
            "double": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "real": SodaDataTypeName.FLOAT,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp_ntz": SodaDataTypeName.TIMESTAMP,
            TIMESTAMP_WITHOUT_TIME_ZONE: SodaDataTypeName.TIMESTAMP,
            "datetime": SodaDataTypeName.TIMESTAMP,
            "timestamp_tz": SodaDataTypeName.TIMESTAMP_TZ,
            TIMESTAMP_WITH_TIME_ZONE: SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp_ltz": SodaDataTypeName.TIMESTAMP_TZ,
            TIMESTAMP_WITH_LOCAL_TIME_ZONE: SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "boolean": SodaDataTypeName.BOOLEAN,
        }

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in [
            "timestamp",
            "timestamp_ntz",
            TIMESTAMP_WITHOUT_TIME_ZONE,
            "timestamp_tz",
            TIMESTAMP_WITH_TIME_ZONE,
            "timestamp_ltz",
            TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        ]

    # Snowflake's native max VARCHAR width — reported as CHARACTER_MAXIMUM_LENGTH
    # for any unbounded VARCHAR / TEXT column in INFORMATION_SCHEMA.COLUMNS.
    _UNBOUNDED_VARCHAR_LENGTH = 16777216

    def build_column_metadatas_from_query_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        column_metadatas = super().build_column_metadatas_from_query_result(query_result)
        for cm in column_metadatas:
            # Snowflake reports unbounded VARCHAR / TEXT columns with
            # CHARACTER_MAXIMUM_LENGTH=16777216 in INFORMATION_SCHEMA — its native
            # max. Normalize that to canonical TEXT (no length) so consumers of
            # this column metadata see a length-less TEXT type rather than a
            # literal VARCHAR(16777216) that some dialects can't represent.
            if (
                cm.sql_data_type is not None
                and cm.sql_data_type.name.lower() in ("varchar", "char", "text", "string")
                and cm.sql_data_type.character_maximum_length == self._UNBOUNDED_VARCHAR_LENGTH
            ):
                cm.sql_data_type.name = "text"
                cm.sql_data_type.character_maximum_length = None
        return column_metadatas

    def supports_sampler(self, sampler_type: SamplerType) -> bool:
        return sampler_type in (SamplerType.ABSOLUTE_LIMIT, SamplerType.PERCENTAGE)

    def _build_random_sql(self, random: RANDOM) -> str:
        return "UNIFORM(0::FLOAT, 1::FLOAT, RANDOM())"

    def _build_sample_sql(self, sampler_type: SamplerType, sample_size: Number) -> str:
        if sampler_type is SamplerType.ABSOLUTE_LIMIT:
            return f"TABLESAMPLE ({int(sample_size)} ROWS)"
        elif sampler_type is SamplerType.PERCENTAGE:
            return f"TABLESAMPLE ({sample_size})"
        else:
            raise ValueError(f"Unsupported sampler type: {sampler_type.name}")

    ###
    # Data plane statements
    ###

    # MERGE names its incoming rows; SOURCE_COLUMN renders as this alias.
    _MERGE_SOURCE_ALIAS = "src"
    _MERGE_DEFAULT_TARGET_ALIAS = "tgt"

    def _build_upsert_sql(self, upsert: UPSERT) -> str:
        return self._build_merge_sql(upsert, self._build_upsert_rows_sql(upsert), name_source_columns=False)

    def _build_upsert_via_select_sql(self, upsert_via_select: UPSERT_VIA_SELECT) -> str:
        # A WITH stays inside the USING subquery: Snowflake refuses one in front of a MERGE.
        select_sql: str = self.build_select_sql(upsert_via_select.select_elements, add_semicolon=False)
        return self._build_merge_sql(upsert_via_select, select_sql, name_source_columns=True)

    def _build_upsert_rows_sql(self, upsert: UPSERT) -> str:
        """The rows as SELECTs joined by UNION ALL, the first naming the columns. Snowflake documents VALUES
        in an INSERT or a FROM as refusing some expressions, PARSE_JSON among them; a SELECT takes them, so
        whether a MERGE's VALUES does is moot."""
        if not upsert.values:
            raise ValueError("An upsert must have at least one row")
        column_names: list[str] = [self.quote_default(self._upsert_column_name(column)) for column in upsert.columns]
        selects_sql: list[str] = []
        for index, row in enumerate(upsert.values):
            # Only the first row names its columns, so a later row of another length would still render.
            if len(row.values) != len(column_names):
                raise ValueError(
                    f"An upsert row must have one value per column, got {len(row.values)} for "
                    f"{len(column_names)} in values[{index}]"
                )
            values_sql: list[str] = [self.literal(value) for value in row.values]
            if index == 0:
                values_sql = [
                    f"{value_sql} AS {column_name}" for value_sql, column_name in zip(values_sql, column_names)
                ]
            selects_sql.append("SELECT " + ", ".join(values_sql))
        return "\nUNION ALL ".join(selects_sql)

    def _build_merge_sql(self, upsert: UPSERT | UPSERT_VIA_SELECT, source_sql: str, name_source_columns: bool) -> str:
        # Unmatched duplicate keys all insert; matched ones fail only on UPDATE with ERROR_ON_NONDETERMINISTIC_MERGE.
        target_alias: str = upsert.alias or self._MERGE_DEFAULT_TARGET_ALIAS
        if target_alias.lower() == self._MERGE_SOURCE_ALIAS:
            raise UnsupportedSqlStatementError(
                f"{type(self).__name__} does not support an upsert target alias {target_alias!r}: "
                f"it names the incoming rows"
            )
        target: str = self.quote_default(target_alias)
        source: str = self.quote_default(self._MERGE_SOURCE_ALIAS)
        column_names: list[str] = [self.quote_default(self._upsert_column_name(column)) for column in upsert.columns]
        columns_sql: str = ", ".join(column_names)
        key_condition_sql: str = " AND ".join(
            f"{target}.{key} = {source}.{key}"
            for key in (self.quote_default(self._upsert_column_name(column)) for column in upsert.key_columns)
        )
        source_columns_sql: str = f" ({columns_sql})" if name_source_columns else ""
        lines: list[str] = [
            f"MERGE INTO {upsert.fully_qualified_table_name} AS {target}",
            f"USING (\n{source_sql}\n) AS {source}{source_columns_sql}",
            f"ON ({key_condition_sql})",
        ]
        if upsert.update_assignments:
            lines.append(f"WHEN MATCHED THEN UPDATE SET {self._build_assignments_sql(upsert.update_assignments)}")
        source_values_sql: str = ", ".join(f"{source}.{column_name}" for column_name in column_names)
        lines.append(f"WHEN NOT MATCHED THEN INSERT ({columns_sql}) VALUES ({source_values_sql})")
        return "\n".join(lines)

    @staticmethod
    def _upsert_column_name(column: COLUMN | str) -> str:
        column_name = column.name if isinstance(column, COLUMN) else column
        if not isinstance(column_name, str):
            raise ValueError(f"An upsert column must be named, got {column_name!r}")
        return column_name

    def _build_source_column_sql(self, source_column: SOURCE_COLUMN) -> str:
        return f"{self.quote_default(self._MERGE_SOURCE_ALIAS)}.{self.quote_default(source_column.name)}"

    def _begin_read_only_transaction_sql(self) -> Optional[str]:
        # No read-only transaction: rollback_sql undoes the DML written in this one, until a DDL statement
        # commits it, leaving the session in autocommit (AUTOCOMMIT on, the default): DML after it commits at
        # once and ROLLBACK does nothing.
        return "BEGIN TRANSACTION"

    def _rollback_sql(self) -> Optional[str]:
        return "ROLLBACK"

    def _build_timestamp_iso_text_sql(self, timestamp_iso_text: TIMESTAMP_ISO_TEXT) -> str:
        # Double-quoted text in a TO_CHAR format is output verbatim. FF6 truncates to microseconds.
        iso_format: str = 'YYYY-MM-DD"T"HH24:MI:SS'
        if timestamp_iso_text.fractional_seconds:
            iso_format += ".FF6"
        if timestamp_iso_text.utc_suffix:
            iso_format += '"+00:00"'
        expression_sql: str = self.build_expression_sql(timestamp_iso_text.expression)
        return f"TO_CHAR({expression_sql}, {self.literal_string(iso_format)})"

    def _build_date_iso_text_sql(self, date_iso_text: DATE_ISO_TEXT) -> str:
        return f"TO_CHAR({self.build_expression_sql(date_iso_text.expression)}, {self.literal_string('YYYY-MM-DD')})"

    def _build_to_timezone_sql(self, to_timezone: TO_TIMEZONE) -> str:
        # The two-argument CONVERT_TIMEZONE returns a TIMESTAMP_TZ in the zone; the cast keeps its wall clock.
        expression_sql: str = self.build_expression_sql(to_timezone.expression)
        return f"CAST(CONVERT_TIMEZONE({self.literal_string(to_timezone.timezone)}, {expression_sql}) AS TIMESTAMP_NTZ)"

    def _build_date_trunc_sql(self, date_trunc: DATE_TRUNC) -> str:
        return f"DATE_TRUNC({self.literal_string(date_trunc.unit)}, {self.build_expression_sql(date_trunc.expression)})"

    def _build_regexp_replace_sql(self, regexp_replace: REGEXP_REPLACE) -> str:
        # Every match is replaced by default. escape_string doubles a backreference's backslash, which
        # Snowflake reads back as one.
        expression_sql: str = self.build_expression_sql(regexp_replace.expression)
        pattern_sql: str = self.literal_string(regexp_replace.pattern)
        replacement_sql: str = self.literal_string(regexp_replace.replacement)
        return f"REGEXP_REPLACE({expression_sql}, {pattern_sql}, {replacement_sql})"

    def _build_epoch_seconds_sql(self, epoch_seconds: EPOCH_SECONDS) -> str:
        # Snowflake has no interval type: a timestamp difference is TIMESTAMPDIFF's count of one unit.
        raise self._unsupported_sql_statement_error("EPOCH_SECONDS")

    def _build_json_merge_sql(self, json_merge: JSON_MERGE) -> str:
        # No built-in merges two OBJECT values without casting them to MAP; a caller sets the keys one by one.
        raise self._unsupported_sql_statement_error("JSON_MERGE")
