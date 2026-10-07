import logging
import re
from copy import deepcopy
from datetime import date, datetime
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import UnsupportedSqlStatementError
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    ADD_INTERVAL,
    ANALYZE_TABLE,
    COLUMN,
    COUNT,
    CREATE_INDEX_IF_NOT_EXISTS,
    CREATE_SCHEMA_IF_NOT_EXISTS,
    CREATE_TABLE,
    CREATE_TABLE_AS_SELECT,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CREATE_VIEW,
    CURRENT_TIMESTAMP,
    DATE_ISO_TEXT,
    DATE_TRUNC,
    DELETE,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    DROP_VIEW,
    DROP_VIEW_IF_EXISTS,
    EPOCH_SECONDS,
    FROM,
    INSERT_INTO,
    INSERT_INTO_VIA_SELECT,
    INTO,
    JOIN,
    JSON_MERGE,
    LEFT_INNER_JOIN,
    LENGTH,
    LIMIT,
    OFFSET,
    PERCENTILE_WITHIN_GROUP,
    RANDOM,
    REGEXP_REPLACE,
    SOURCE_COLUMN,
    STRING_HASH,
    TIME_DELTA,
    TIMESTAMP_ISO_TEXT,
    TO_TIMEZONE,
    TUPLE,
    UPDATE,
    UPSERT,
    UPSERT_VIA_SELECT,
    VALUES,
    WITH,
    seconds_per_time_bucket,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSource as SqlServerDataSourceModel,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import SqlServerDataSourceConnection

logger: logging.Logger = soda_logger


# APPROX_PERCENTILE_DISC needs SQL Server 2022+ on-prem, or Azure SQL Database /
# Managed Instance (which report a legacy ProductMajorVersion).
# So does DATETRUNC.
SQLSERVER_2022_MAJOR_VERSION = 16
AZURE_SQL_DATABASE_ENGINE_EDITION = 5
AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION = 8
# REGEXP_REPLACE needs SQL Server 2025+ on-prem, or Azure SQL Database.
SQLSERVER_2025_MAJOR_VERSION = 17
SQLSERVER_RELEASE_BY_MAJOR_VERSION = {
    SQLSERVER_2022_MAJOR_VERSION: "SQL Server 2022",
    SQLSERVER_2025_MAJOR_VERSION: "SQL Server 2025",
}
AZURE_ENGINE_NAME_BY_EDITION = {
    AZURE_SQL_DATABASE_ENGINE_EDITION: "Azure SQL Database",
    AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION: "Azure SQL Managed Instance",
}

# A three-part name, capturing the database. Each part is bracket-quoted, with ]] for a literal ], or plain.
_NAME_PART_PATTERN = r"(?:\[(?:[^\]]|\]\])*\]|[^.\[\]]*)"
THREE_PART_NAME_PATTERN = re.compile(rf"({_NAME_PART_PATTERN})\.{_NAME_PART_PATTERN}\.{_NAME_PART_PATTERN}")


class SqlServerDataSourceImpl(DataSourceImpl, model_class=SqlServerDataSourceModel):
    def __init__(self, data_source_model: SqlServerDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)
        # A live connection supplied at construction (e.g. a bulk-insert copy)
        # already carries detected server facts; propagate them right away.
        self._sync_dialect_server_info()

    def _create_sql_dialect(self) -> SqlDialect:
        return SqlServerSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SqlServerDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def open_connection(self) -> None:
        super().open_connection()
        self._sync_dialect_server_info()

    def _sync_dialect_server_info(self) -> None:
        """Copy the connection's detected engine facts onto the dialect, which
        derives version-dependent capabilities from them (see
        SqlServerSqlDialect.supports_percentile_within_group).

        Runs at connection-open time only; the dialect must NOT read the connection
        during SQL generation — in snapshot replay the connection is a lazy wrapper
        whose attribute access opens a real connection, so touching it while
        building SQL breaks replay.

        Gate on the concrete connection type rather than duck-typing the attributes:
        a replay SnapshotDataSourceConnection is NOT a SqlServerDataSourceConnection,
        so isinstance() is False and we never touch its attributes (which would fire
        its __getattr__ fallback and open a real connection). Replay then keeps the
        dialect's None defaults (assume newest engine), which is exactly what
        recorded snapshots expect.
        """
        conn = self.data_source_connection
        if isinstance(conn, SqlServerDataSourceConnection):
            # Guaranteed by every _create_sql_dialect in this hierarchy; the assert
            # only narrows the declared SqlDialect type for the assignments.
            assert isinstance(self.sql_dialect, SqlServerSqlDialect)
            self.sql_dialect.server_major_version = conn.server_major_version
            self.sql_dialect.engine_edition = conn.engine_edition


class SqlServerSqlDialect(SqlDialect, sqlglot_dialect="tsql"):
    DEFAULT_QUOTE_CHAR = "["  # Do not use this! Always use quote_default()
    SODA_DATA_TYPE_SYNONYMS = ((SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR),)
    # T-SQL's page window is `OFFSET m ROWS` then `FETCH NEXT n ROWS ONLY`.
    OFFSET_BEFORE_LIMIT: bool = True
    SUPPORTS_DATA_PLANE_STATEMENTS = True

    def __init__(self):
        super().__init__()
        # Raw engine facts, synced from the live connection at open by
        # SqlServerDataSourceImpl._sync_dialect_server_info. None means no live
        # server facts (pure SQL rendering, unit tests, snapshot replay);
        # capability checks then assume the newest engine.
        self.server_major_version: Optional[int] = None
        self.engine_edition: Optional[int] = None

    def supports_primary_keys(self) -> bool:
        # SQL Server enforces primary keys and reports them through the standard
        # information_schema constraint views, with the standard PRIMARY KEY (...) DDL.
        return True

    def _build_stddev_samp_sql(self, stddev_samp) -> str:
        # T-SQL names the sample standard deviation aggregate STDEV.
        return f"STDEV({self.build_expression_sql(stddev_samp.expression)})"

    def _build_var_samp_sql(self, var_samp) -> str:
        # T-SQL names the sample variance aggregate VAR.
        return f"VAR({self.build_expression_sql(var_samp.expression)})"

    def supports_percentile_within_group(self) -> bool:
        # T-SQL exposes percentiles as an aggregate only via APPROX_PERCENTILE_DISC:
        # SQL Server 2022+ (ProductMajorVersion >= 16), Azure SQL Database, or Azure
        # SQL Managed Instance (both report a legacy ProductMajorVersion, hence the
        # edition check). Synapse dedicated pools have no percentile aggregate at
        # all; the Synapse dialect pins this to False.
        if self.server_major_version is None and self.engine_edition is None:
            return True  # no live server facts: assume the newest engine
        return (
            self.server_major_version is not None and self.server_major_version >= SQLSERVER_2022_MAJOR_VERSION
        ) or self.engine_edition in (
            AZURE_SQL_DATABASE_ENGINE_EDITION,
            AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION,
        )

    def _build_select_sql_lines(self, select_elements: list) -> list[str]:
        # Use the default implementation, but we need to handle the case where the select elements contain a LIMIT statement.
        select_sql_lines: list[str] = super()._build_select_sql_lines(select_elements)
        if self.__requires_select_top(select_elements):
            limit_element: LIMIT = [
                select_element for select_element in select_elements if isinstance(select_element, LIMIT)
            ][0]
            # T-SQL grammar is `SELECT [ALL | DISTINCT] [TOP n] <fields>`, so TOP goes after
            # DISTINCT when the base rendered a distinct select.
            select_prefix: str = "SELECT DISTINCT " if select_sql_lines[0].startswith("SELECT DISTINCT ") else "SELECT "
            select_sql_lines[0] = select_sql_lines[0].replace(
                select_prefix, f"{select_prefix}TOP {limit_element.limit} ", 1
            )
        return select_sql_lines

    def __requires_select_top(self, select_elements: list) -> bool:
        # We require TOP when there is a LIMIT statement and no OFFSET statement.
        return any(isinstance(select_element, LIMIT) for select_element in select_elements) and not any(
            isinstance(select_element, OFFSET) for select_element in select_elements
        )

    def _build_limit_line(self, select_elements: list) -> Optional[str]:
        # First, check if there is a LIMIT statement in the select elements.
        limit_statement_present = any(isinstance(select_element, LIMIT) for select_element in select_elements)
        if not limit_statement_present:
            return None

        # Check if there is an OFFSET statement in the select elements. If so, use the default logic.
        uses_offset = any(isinstance(select_element, OFFSET) for select_element in select_elements)
        if uses_offset:
            return super()._build_limit_line(select_elements)
        else:
            return None  # This case (limit, but no offset) is handled by the _build_select_sql_lines method; it adds TOP N instead of FETCH NEXT.

    def literal_date(self, date: date):
        """Technically dates can be passed directly as strings, but this is more explicit."""
        return f"CAST('{date.isoformat()}' AS DATE)"

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

    def _build_count_sql(self, count: COUNT) -> str:
        # T-SQL COUNT returns INT and overflows above 2,147,483,647 rows. COUNT_BIG is the
        # BIGINT-returning equivalent with identical null/distinct semantics.
        return f"COUNT_BIG({self.build_expression_sql(count.expression)})"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"'{datetime_in_iso8601}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETRUNC(DAY, {timestamp_literal})"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"DATEADD(DAY, 1, {timestamp_literal})"

    def literal_timestamp_typed(self, dt: datetime) -> str:
        """T-SQL has no TIMESTAMP '...' literal (TIMESTAMP is the deprecated
        rowversion type), so cast the string form to DATETIME2 to keep the
        arithmetic operand typed —
        https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql."""
        return f"CAST('{self._typed_timestamp_str(dt)}' AS DATETIME2)"

    # Singular unit names for DATEADD.
    _TIME_BUCKET_UNIT_NAMES: dict = {
        "weeks": "WEEK",
        "days": "DAY",
        "hours": "HOUR",
        "seconds": "SECOND",
    }

    def _build_time_delta_sql(self, time_delta: TIME_DELTA) -> str:
        """T-SQL DATEDIFF counts crossed boundaries of the given unit, so
        compute the difference in SECONDS and divide by the seconds-per-
        interval. T-SQL int/int division truncates toward zero, which equals
        the FLOOR of the other dialects only for deltas >= 0 — callers must
        guarantee non-negative deltas (the MM window filter does).

        DATEDIFF(second, ...) returns int and overflows for spans > ~68
        years; switch to DATEDIFF_BIG if that ever bites."""
        start_sql: str = self.build_expression_sql(time_delta.start)
        end_sql: str = self.build_expression_sql(time_delta.end)
        multiplier: int = seconds_per_time_bucket(time_delta.unit, time_delta.count)
        # Parenthesized so the form stays self-contained if a caller embeds
        # TIME_DELTA in larger arithmetic (every other dialect wraps in FLOOR/cast).
        return f"(DATEDIFF(second, {start_sql}, {end_sql}) / {multiplier})"

    def _build_add_interval_sql(self, add_interval: ADD_INTERVAL) -> str:
        timestamp_sql: str = self.build_expression_sql(add_interval.timestamp)
        count_sql: str = self.build_expression_sql(add_interval.count_expression)
        unit_name: str = self._TIME_BUCKET_UNIT_NAMES[add_interval.unit]
        return f"DATEADD({unit_name}, {count_sql}, {timestamp_sql})"

    def _build_percentile_within_group_sql(self, percentile_within_group: PERCENTILE_WITHIN_GROUP) -> str:
        """T-SQL PERCENTILE_DISC is a window function only; the aggregate form
        is APPROX_PERCENTILE_DISC (SQL Server 2022+/Azure SQL/Fabric,
        https://learn.microsoft.com/en-us/sql/t-sql/functions/approx-percentile-disc-transact-sql)."""
        expression_sql: str = self.build_expression_sql(percentile_within_group.expression)
        return f"APPROX_PERCENTILE_DISC({percentile_within_group.percentile}) WITHIN GROUP (ORDER BY {expression_sql})"

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return f"CHECKSUM{super()._build_tuple_sql(tuple)}"
        if tuple.check_context(VALUES):
            # in built_cte_values_sql, elements are dropped in top-level select statement, so can't use parentheses
            return ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
        return super()._build_tuple_sql(tuple)

    def _rewrite_regex_pattern(self, regex_pattern: str) -> str:
        # alpha expansion doesn't work properly for case sensitive ranges in SQLServer
        # this is quite a hack to fit the common use-cases.  generally regex's are only
        # partially supported anyway
        regex_pattern = regex_pattern.replace("a-z", "abcdefghijklmnopqrstuvwxyz")
        regex_pattern = regex_pattern.replace("A-Z", "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        # PATINDEX matches a substring, so the pattern is wrapped rather than anchored.
        # Wrapping here rather than around the rendered literal keeps the % inside the
        # quotes that the base escaping puts on.
        return f"%{regex_pattern}%"

    def _regex_like_sql(self, expression: str, pattern: str) -> str:
        # collations define rules for sorting strings and distinguishing similar characters
        # see: https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver17
        # CS: Case sensitive; AS: Accent sensitive
        # The default is SQL_Latin1_General_Cp1_CI_AS (case-insensitive), we replcae with a case sensitive collation
        return f"PATINDEX ({pattern}, {expression} COLLATE SQL_Latin1_General_Cp1_CS_AS) > 0"

    def supports_regex_advanced(self) -> bool:
        return False

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return "\nUNION ALL\n".join(["SELECT " + self.build_expression_sql(value) for value in values.values])

    # No select_all_paginated_sql override: the base composes the same elements, and the
    # OFFSET-before-FETCH order of the rendered clause is owned by this dialect's
    # build_select_sql — the statement-list order never influenced rendering.

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

    # SQL Server's datetime2 / datetimeoffset / time accept a fractional-seconds
    # precision of 0..7. Any higher value is rejected at CREATE TABLE time.
    _MAX_DATETIME_PRECISION = 7

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        # Clamp datetime precision to SQL Server's max. Cross-source flows from sources
        # with higher native precision (e.g. Snowflake's TIMESTAMP_NTZ defaults to 9)
        # would otherwise produce e.g. `datetime2(9)` and fail CREATE TABLE.
        if create_table_column.type.name.lower() in ("datetime2", "datetimeoffset", "time"):
            if (
                create_table_column.type.datetime_precision is not None
                and create_table_column.type.datetime_precision > self._MAX_DATETIME_PRECISION
            ):
                create_table_column.type.datetime_precision = self._MAX_DATETIME_PRECISION
        return super()._build_create_table_column_type(create_table_column)

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
        self, create_table_as_select: CREATE_TABLE_AS_SELECT, add_semicolon: bool = True, add_parenthesis: bool = True
    ) -> str:
        # Copy the select elements and insert an INTO with the same table name as the create table as select statement
        select_elements = create_table_as_select.select_elements.copy()
        select_elements += [INTO(fully_qualified_table_name=create_table_as_select.fully_qualified_table_name)]
        result_sql: str = self.build_select_sql(select_elements, add_semicolon=add_semicolon)
        return result_sql

    def build_drop_view_sql(self, drop_view: DROP_VIEW | DROP_VIEW_IF_EXISTS, add_semicolon: bool = True) -> str:
        # SqlServer does not allow for the database name to be specified in the view name, so we need to drop it.
        drop_view_copy = deepcopy(drop_view)  # Copy the object so we don't modify the original object
        # Drop the first prefix (database name) from the fully qualified view name
        drop_view_copy.fully_qualified_view_name = ".".join(drop_view_copy.fully_qualified_view_name.split(".")[1:])
        return super().build_drop_view_sql(drop_view_copy, add_semicolon)

    def build_create_view_sql(
        self, create_view: CREATE_VIEW, add_semicolon: bool = True, add_parenthesis: bool = True
    ) -> str:
        # SqlServer does not allow for the database name to be specified in the view name, so we need to drop it.
        create_view_copy = deepcopy(create_view)  # Copy the object so we don't modify the original object
        # Drop the first prefix (database name) from the fully qualified view name
        create_view_copy.fully_qualified_view_name = ".".join(create_view_copy.fully_qualified_view_name.split(".")[1:])
        return super().build_create_view_sql(create_view_copy, add_semicolon, add_parenthesis=False)

    def _build_random_sql(self, random: RANDOM) -> str:
        return "ABS(CAST(CHECKSUM(NEWID()) AS FLOAT)) / 2147483648.0"

    ###
    # Data plane statements
    ###
    # Every override below is inherited by the Fabric and Synapse dialects, which do not set
    # SUPPORTS_DATA_PLANE_STATEMENTS: the base gates each hook on the flag, except
    # _build_current_timestamp_sql, which therefore checks the flag itself.

    # MERGE names its incoming rows; SOURCE_COLUMN renders as this alias.
    _MERGE_SOURCE_ALIAS = "src"
    _MERGE_DEFAULT_TARGET_ALIAS = "tgt"

    def _build_update_sql(self, update: UPDATE) -> str:
        if not update.alias:
            if update.from_elements:
                raise self._unaliased_target_error("UPDATE", "from_elements")
            return super()._build_update_sql(update)
        lines: list[str] = [
            f"UPDATE {self.quote_default(update.alias)}",
            f"SET {self._build_assignments_sql(update.assignments)}",
        ]
        lines.extend(
            self._build_target_and_sources_sql_lines(
                update.fully_qualified_table_name, update.alias, update.from_elements
            )
        )
        if update.where is not None:
            lines.extend(self._build_where_sql_lines([update.where]))
        return "\n".join(lines)

    def _build_delete_sql(self, delete: DELETE) -> str:
        if not delete.alias:
            if delete.using_elements:
                raise self._unaliased_target_error("DELETE", "using_elements")
            return super()._build_delete_sql(delete)
        lines: list[str] = [f"DELETE {self.quote_default(delete.alias)}"]
        lines.extend(
            self._build_target_and_sources_sql_lines(
                delete.fully_qualified_table_name, delete.alias, delete.using_elements
            )
        )
        if delete.where is not None:
            lines.extend(self._build_where_sql_lines([delete.where]))
        return "\n".join(lines)

    def _unaliased_target_error(self, node_name: str, sources_field: str) -> UnsupportedSqlStatementError:
        return UnsupportedSqlStatementError(
            f"{type(self).__name__} does not support {node_name} with {sources_field} and no alias: T-SQL binds "
            f"an unaliased target to a reference to the same table among the further tables"
        )

    def _build_target_and_sources_sql_lines(
        self, fully_qualified_table_name: str, alias: str, elements: Optional[list[FROM | JOIN]]
    ) -> list[str]:
        """``FROM <target> AS <alias>`` followed by the further tables, laid out as the base lays out an
        UPDATE's FROM clause: plain FROM elements comma-separated, each JOIN on its own line."""
        lines: list[str] = [f"FROM {self._build_statement_target_sql(fully_qualified_table_name, alias)}"]
        continuation: str = " " * len("FROM ")
        for element in elements or []:
            if isinstance(element, (LEFT_INNER_JOIN, JOIN)):
                lines.append(f"{continuation}{self._build_join_part(element)}")
            else:
                lines[-1] += ","
                lines.append(f"{continuation}{self._build_from_part(element)}")
        return lines

    def build_upsert_sql(self, upsert: UPSERT, add_semicolon: Optional[bool] = None) -> str:
        # The MERGE carries its own semicolon: T-SQL requires it (error 10713) whatever the caller asks.
        return super().build_upsert_sql(upsert, add_semicolon=False)

    def build_upsert_via_select_sql(
        self, upsert_via_select: UPSERT_VIA_SELECT, add_semicolon: Optional[bool] = None
    ) -> str:
        return super().build_upsert_via_select_sql(upsert_via_select, add_semicolon=False)

    def _build_upsert_sql(self, upsert: UPSERT) -> str:
        return self._build_merge_sql(upsert, self._build_insert_into_values_sql(upsert).strip())

    def _build_upsert_via_select_sql(self, upsert_via_select: UPSERT_VIA_SELECT) -> str:
        # A CTE cannot open a MERGE source (error 156), so it moves in front of the MERGE.
        with_elements, select_elements = self._split_with_elements(upsert_via_select.select_elements)
        merge_sql: str = self._build_merge_sql(
            upsert_via_select, self.build_select_sql(select_elements, add_semicolon=False)
        )
        return "\n".join([*self._build_cte_sql_lines(with_elements), merge_sql])

    @staticmethod
    def _split_with_elements(select_elements: list) -> tuple[list[WITH], list]:
        """The WITH elements of a statement's SELECT, which T-SQL puts in front of the whole statement, and
        the other elements."""
        with_elements: list[WITH] = [element for element in select_elements if isinstance(element, WITH)]
        other_elements: list = [element for element in select_elements if not isinstance(element, WITH)]
        return with_elements, other_elements

    def _build_merge_sql(self, upsert: UPSERT | UPSERT_VIA_SELECT, source_sql: str) -> str:
        """HOLDLOCK keeps the key range locked from the match to the insert, so a concurrent upsert of the
        same key waits instead of failing on the primary key."""
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
        lines: list[str] = [
            f"MERGE INTO {upsert.fully_qualified_table_name} WITH (HOLDLOCK) AS {target}",
            f"USING (\n{source_sql}\n) AS {source} ({columns_sql})",
            f"ON ({key_condition_sql})",
        ]
        if upsert.update_assignments:
            lines.append(f"WHEN MATCHED THEN UPDATE SET {self._build_assignments_sql(upsert.update_assignments)}")
        source_values_sql: str = ", ".join(f"{source}.{column_name}" for column_name in column_names)
        lines.append(f"WHEN NOT MATCHED THEN INSERT ({columns_sql}) VALUES ({source_values_sql});")
        return "\n".join(lines)

    @staticmethod
    def _upsert_column_name(column: COLUMN | str) -> str:
        column_name = column.name if isinstance(column, COLUMN) else column
        if not isinstance(column_name, str):
            raise ValueError(f"An upsert column must be named, got {column_name!r}")
        return column_name

    def _build_source_column_sql(self, source_column: SOURCE_COLUMN) -> str:
        return f"{self.quote_default(self._MERGE_SOURCE_ALIAS)}.{self.quote_default(source_column.name)}"

    def _build_create_index_sql(self, create_index: CREATE_INDEX_IF_NOT_EXISTS) -> Optional[str]:
        # T-SQL has no CREATE INDEX IF NOT EXISTS.
        index_name: str = create_index.index_name
        table_name: str = self._convert_fqn_for_ddl(create_index.fully_qualified_table_name)
        columns_sql: str = self._build_index_columns_sql(create_index.columns)
        return (
            f"IF NOT EXISTS (SELECT 1 FROM {self._sys_indexes_sql(table_name)} "
            f"WHERE name = N{self.literal_string(index_name)} "
            f"AND object_id = OBJECT_ID(N{self.literal_string(table_name)})) "
            f"CREATE INDEX {self.quote_for_ddl(index_name)} ON {table_name} {columns_sql}"
        )

    @staticmethod
    def _sys_indexes_sql(table_name: str) -> str:
        """The sys.indexes of the database OBJECT_ID resolves the table in: the one its name qualifies it
        with, or else the current one."""
        three_part_name = THREE_PART_NAME_PATTERN.fullmatch(table_name)
        if three_part_name and three_part_name.group(1):
            return f"{three_part_name.group(1)}.sys.indexes"
        return "sys.indexes"

    def _build_create_schema_sql(self, create_schema: CREATE_SCHEMA_IF_NOT_EXISTS) -> str:
        # CREATE SCHEMA must be alone in its batch, hence EXEC; the schema lands in the current database.
        schema_name: str = create_schema.prefixes[1]
        create_schema_sql: str = f"CREATE SCHEMA [{schema_name.replace(']', ']]')}]"
        return (
            f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N{self.literal_string(schema_name)}) "
            f"EXEC(N{self.literal_string(create_schema_sql)})"
        )

    def _build_analyze_table_sql(self, analyze_table: ANALYZE_TABLE) -> Optional[str]:
        return f"UPDATE STATISTICS {self._convert_fqn_for_ddl(analyze_table.fully_qualified_table_name)}"

    def _begin_read_only_transaction_sql(self) -> Optional[str]:
        # T-SQL has no read-only transaction; rollback_sql undoes whatever was written in this one.
        return "BEGIN TRANSACTION"

    def _rollback_sql(self) -> Optional[str]:
        # An error can already have ended the transaction, and a ROLLBACK without one fails (error 3903).
        return "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"

    def _build_current_timestamp_sql(self, current_timestamp: CURRENT_TIMESTAMP) -> str:
        if not self.SUPPORTS_DATA_PLANE_STATEMENTS:
            return super()._build_current_timestamp_sql(current_timestamp)
        # T-SQL's CURRENT_TIMESTAMP is the server's local time without an offset.
        return "SYSDATETIMEOFFSET()"

    def _build_timestamp_iso_text_sql(self, timestamp_iso_text: TIMESTAMP_ISO_TEXT) -> str:
        # Through datetime2(7), so a date or a text renders as a timestamp does.
        timestamp_sql: str = f"CAST({self.build_expression_sql(timestamp_iso_text.expression)} AS datetime2(7))"
        iso_text_sql: str
        if timestamp_iso_text.fractional_seconds:
            # Style 126 drops a zero fraction; style 121 keeps it, with a space where ISO has the T. LEFT
            # truncates the seventh digit as the whole-second form does, where datetime2(6) would round.
            iso_text_sql = f"STUFF(LEFT(CONVERT(varchar(27), {timestamp_sql}, 121), 26), 11, 1, 'T')"
        else:
            iso_text_sql = f"CONVERT(varchar(19), {timestamp_sql}, 126)"
        if timestamp_iso_text.utc_suffix:
            # + propagates NULL; CONCAT would return the bare suffix.
            return f"({iso_text_sql} + '+00:00')"
        return iso_text_sql

    def _build_date_iso_text_sql(self, date_iso_text: DATE_ISO_TEXT) -> str:
        return f"CONVERT(char(10), {self.build_expression_sql(date_iso_text.expression)}, 23)"

    def _build_to_timezone_sql(self, to_timezone: TO_TIMEZONE) -> str:
        # AT TIME ZONE takes Windows zone names, so UTC is the one zone rendered.
        if to_timezone.timezone.lower() not in ("utc", "etc/utc"):
            raise UnsupportedSqlStatementError(
                f"{type(self).__name__} does not support TO_TIMEZONE to {to_timezone.timezone!r}: only UTC"
            )
        return f"CAST(SWITCHOFFSET({self.build_expression_sql(to_timezone.expression)}, '+00:00') AS datetime2(7))"

    def _build_date_trunc_sql(self, date_trunc: DATE_TRUNC) -> str:
        self._require_engine(
            node_name="DATE_TRUNC",
            function_name="DATETRUNC",
            major_version=SQLSERVER_2022_MAJOR_VERSION,
            azure_engine_editions=(AZURE_SQL_DATABASE_ENGINE_EDITION, AZURE_SQL_MANAGED_INSTANCE_ENGINE_EDITION),
        )
        return f"DATETRUNC({date_trunc.unit.upper()}, {self.build_expression_sql(date_trunc.expression)})"

    def _build_regexp_replace_sql(self, regexp_replace: REGEXP_REPLACE) -> str:
        # A Managed Instance has the regex functions only under some update policies, which its facts
        # do not tell apart.
        self._require_engine(
            node_name="REGEXP_REPLACE",
            function_name="REGEXP_REPLACE",
            major_version=SQLSERVER_2025_MAJOR_VERSION,
            azure_engine_editions=(AZURE_SQL_DATABASE_ENGINE_EDITION,),
        )
        expression_sql: str = self.build_expression_sql(regexp_replace.expression)
        pattern_sql: str = self.literal_string(regexp_replace.pattern)
        replacement_sql: str = self.literal_string(regexp_replace.replacement)
        return f"REGEXP_REPLACE({expression_sql}, {pattern_sql}, {replacement_sql})"

    def _build_epoch_seconds_sql(self, epoch_seconds: EPOCH_SECONDS) -> str:
        # T-SQL has no interval type: a timestamp difference is DATEDIFF's count of one unit.
        raise self._unsupported_sql_statement_error("EPOCH_SECONDS")

    def _build_json_merge_sql(self, json_merge: JSON_MERGE) -> str:
        # No T-SQL function merges two JSON objects; a caller sets the keys one by one with JSON_MODIFY.
        raise self._unsupported_sql_statement_error("JSON_MERGE")

    def _require_engine(
        self,
        node_name: str,
        function_name: str,
        major_version: int,
        azure_engine_editions: tuple[int, ...],
    ) -> None:
        """Raises unless the connected engine has ``function_name``: from ``major_version`` on premises,
        and on the ``azure_engine_editions``, which report a legacy major version. Without server facts
        (rendering only, snapshot replay) the newest engine is assumed, as in
        supports_percentile_within_group."""
        if self.server_major_version is None and self.engine_edition is None:
            return
        if (
            self.server_major_version is not None and self.server_major_version >= major_version
        ) or self.engine_edition in azure_engine_editions:
            return
        engine_names: list[str] = [
            SQLSERVER_RELEASE_BY_MAJOR_VERSION[major_version],
            *(AZURE_ENGINE_NAME_BY_EDITION[edition] for edition in azure_engine_editions),
        ]
        raise UnsupportedSqlStatementError(
            f"{type(self).__name__} does not support {node_name} on this server (major version "
            f"{self.server_major_version}, engine edition {self.engine_edition}): {function_name} needs "
            f"{', '.join(engine_names[:-1])} or {engine_names[-1]}"
        )
