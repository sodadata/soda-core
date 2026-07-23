import logging
from dataclasses import dataclass
from typing import Optional

from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import (
    ADD_INTERVAL,
    COLUMN,
    CONCAT_WS,
    COUNT,
    DISTINCT,
    LITERAL,
    PERCENTILE_WITHIN_GROUP,
    RANDOM,
    REGEX_LIKE,
    STRING_HASH,
    TIME_DELTA,
    TUPLE,
    UNION,
    UNION_ALL,
    VALUES,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery

logger: logging.Logger = soda_logger


@dataclass
class BigQueryDataSourceNamespace(DataSourceNamespace):
    project_id: str
    dataset: str
    location: Optional[str] = None

    def get_namespace_elements(self) -> list[str]:
        if self.location:
            return [self.location, self.project_id]
        else:
            return [self.project_id, self.dataset]

    def get_database_for_metadata_query(self) -> str | None:
        return self.project_id

    def get_schema_for_metadata_query(self) -> str:
        return self.dataset


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)
        self.cached_location = None

    def _create_sql_dialect(self) -> SqlDialect:
        return BigQuerySqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return BigQueryDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def get_location(self) -> str:
        if self.cached_location is not None:
            location = self.cached_location
        elif self.data_source_model.connection_properties.location is not None:
            location = self.data_source_model.connection_properties.location
        else:
            result = self.execute_query("SELECT @@location")
            location = result.rows[0][0]
            logger.info(f"Detected BigQuery location: {location}")
            self.cached_location = location
        return location

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        super_metadata_tables_query = MetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            prefixes=[f"region-{self.get_location()}"],
        )
        return super_metadata_tables_query

    def _build_columns_metadata_namespace(self, prefixes: list[str]) -> DataSourceNamespace:
        return BigQueryDataSourceNamespace(project_id=prefixes[0], dataset=prefixes[1])

    def _build_table_namespace_for_schema_query(self, prefixes: list[str]) -> tuple[DataSourceNamespace, str]:
        table_namespace: DataSourceNamespace = BigQueryDataSourceNamespace(
            project_id=prefixes[0],
            dataset=None,  # Schema existence queries need project-level INFORMATION_SCHEMA
        )
        schema_name = self.extract_schema_from_prefix(prefixes)
        if schema_name is None:
            raise ValueError(f"Cannot determine schema name from prefixes: {prefixes}")

        return table_namespace, schema_name


class BigQuerySqlDialect(SqlDialect, sqlglot_dialect="bigquery"):
    DEFAULT_QUOTE_CHAR = "`"

    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.INTEGER, SodaDataTypeName.BIGINT, SodaDataTypeName.SMALLINT),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP, SodaDataTypeName.TIMESTAMP_TZ),  # Bigquery does not support timezones
        (SodaDataTypeName.FLOAT, SodaDataTypeName.DOUBLE),
    )

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeName.CHAR: "string",  # BigQuery only has STRING
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.TEXT: "string",  # alias for varchar
            SodaDataTypeName.SMALLINT: "int64",  # all integers → INT64
            SodaDataTypeName.INTEGER: "int64",
            SodaDataTypeName.BIGINT: "int64",
            SodaDataTypeName.DECIMAL: "numeric",  # NUMERIC (fixed precision: 38 digits, 9 decimals)
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.FLOAT: "float64",  # FLOAT & DOUBLE → FLOAT64
            SodaDataTypeName.DOUBLE: "float64",
            SodaDataTypeName.TIMESTAMP: "timestamp",  # UTC-based
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",  # still just TIMESTAMP in BigQuery
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "bool",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.VARCHAR,
            "smallint": SodaDataTypeName.SMALLINT,
            "int64": SodaDataTypeName.BIGINT,
            "numeric": SodaDataTypeName.NUMERIC,
            "float64": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "bool": SodaDataTypeName.BOOLEAN,
            # BigQuery DATETIME is a naive (timezone-less) civil timestamp → canonical TIMESTAMP.
            "datetime": SodaDataTypeName.TIMESTAMP,
            "bignumeric": SodaDataTypeName.NUMERIC,
            # Defensive aliases — unreachable via INFORMATION_SCHEMA (BigQuery canonicalizes
            # them to INT64/NUMERIC/BIGNUMERIC/BOOL); consistent with _get_data_type_name_synonyms.
            "int": SodaDataTypeName.BIGINT,
            "integer": SodaDataTypeName.BIGINT,
            "bigint": SodaDataTypeName.BIGINT,
            "tinyint": SodaDataTypeName.SMALLINT,
            "decimal": SodaDataTypeName.NUMERIC,
            "bigdecimal": SodaDataTypeName.NUMERIC,
            "boolean": SodaDataTypeName.BOOLEAN,
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int64", "integer"],
            ["bool", "boolean"],
            ["numeric", "decimal"],
            ["bignumeric", "bigdecimal"],
        ]

    def get_large_numeric_cast_type_name(self) -> Optional[str]:
        """CAST aggregate args to NUMERIC so math over INT64 runs in NUMERIC(38,9),
        not FLOAT64."""
        return "NUMERIC"

    def build_union_sql(self, union: UNION | UNION_ALL, add_semicolon: Optional[bool] = None) -> str:
        """BigQuery rejects bare UNION (requires UNION ALL | UNION DISTINCT).
        Render UNION_ALL as UNION ALL and plain UNION as UNION DISTINCT so the
        set semantics of a plain UNION are preserved for any caller — profiling's
        unioned sets carry disjoint constant metric_ labels, so for that caller
        ALL vs DISTINCT is identical either way.
        """
        add_semicolon = self.apply_default_add_semicolon(add_semicolon)
        union_sql: str = "UNION ALL" if isinstance(union, UNION_ALL) else "UNION DISTINCT"
        return f"\n{union_sql}\n".join(
            [
                f"(\n{self.build_select_sql(select_element, add_semicolon=False)}\n)"
                for select_element in union.select_elements
            ]
        ) + (";" if add_semicolon else "")

    def format_metadata_data_type(self, data_type: str) -> str:
        """Strip the parameter suffix BigQuery INFORMATION_SCHEMA reports (e.g.
        ``NUMERIC(20, 4)``) so type-map lookups match; mirrors the DuckDB dialect."""
        paranthesis_index = data_type.find("(")
        if paranthesis_index != -1:
            return data_type[:paranthesis_index]
        return data_type

    def information_schema_namespace_elements(self, data_source_namespace: BigQueryDataSourceNamespace) -> list[str]:
        return [data_source_namespace.project_id, data_source_namespace.dataset, "INFORMATION_SCHEMA"]

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"TO_JSON_STRING(STRUCT({super()._build_tuple_sql(tuple)}))"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_CONTAINS({expression}, r'{matches.regex_pattern}')"

    def _build_percentile_within_group_sql(self, percentile_within_group: PERCENTILE_WITHIN_GROUP) -> str:
        """BigQuery has no ordered-set aggregates; render as
        ``APPROX_QUANTILES({expr}, 1000)[{int(p*1000)}]``."""
        expression_sql: str = self.build_expression_sql(percentile_within_group.expression)
        quantile_number: int = int(percentile_within_group.percentile * 1000)
        return f"APPROX_QUANTILES({expression_sql}, 1000)[{quantile_number}]"

    # Singular unit names for TIMESTAMP_DIFF/TIMESTAMP_ADD.
    _TIME_BUCKET_UNIT_NAMES: dict = {
        "weeks": "WEEK",
        "days": "DAY",
        "hours": "HOUR",
        "seconds": "SECOND",
    }

    def _build_time_delta_sql(self, time_delta: TIME_DELTA) -> str:
        start_sql: str = self.build_expression_sql(time_delta.start)
        end_sql: str = self.build_expression_sql(time_delta.end)
        unit_name: str = self._TIME_BUCKET_UNIT_NAMES[time_delta.unit]
        sql: str = f"TIMESTAMP_DIFF({end_sql}, {start_sql}, {unit_name})"
        if time_delta.count != 1:
            sql = f"CAST(FLOOR({sql} / {time_delta.count}) AS INT)"
        return sql

    def _build_add_interval_sql(self, add_interval: ADD_INTERVAL) -> str:
        timestamp_sql: str = self.build_expression_sql(add_interval.timestamp)
        count_sql: str = self.build_expression_sql(add_interval.count_expression)
        # BigQuery TIMESTAMP_ADD only accepts MICROSECOND..DAY parts — WEEK is invalid here
        # (it is valid for TIMESTAMP_DIFF, hence the asymmetry with _build_time_delta_sql).
        # Express weekly buckets as INTERVAL <count> * 7 DAY.
        if add_interval.unit == "weeks":
            return f"TIMESTAMP_ADD({timestamp_sql}, INTERVAL {count_sql} * 7 DAY)"
        unit_name: str = self._TIME_BUCKET_UNIT_NAMES[add_interval.unit]
        return f"TIMESTAMP_ADD({timestamp_sql}, INTERVAL {count_sql} {unit_name})"

    def supports_data_type_character_maximum_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_cte_alias_columns(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    # BigQuery's NUMERIC / BIGNUMERIC are fixed-precision (no per-column tuning),
    # so INFORMATION_SCHEMA.COLUMNS doesn't expose NUMERIC_PRECISION / NUMERIC_SCALE.
    # Hard-code BigQuery's documented values so consumers of the column metadata
    # (e.g. cross-source CREATE TABLE on dialects that *do* require explicit
    # precision/scale) carry the correct decimal scale through; otherwise a
    # `numeric` column on the target defaults to scale 0 and silently truncates
    # the fractional part of every value.
    #
    # Note: these overrides intentionally bypass the base layer's
    # `data_type_has_parameter_numeric_*` / `supports_data_type_numeric_*` gates
    # (both return False for BigQuery in the CREATE TABLE path). The asymmetry
    # is source-vs-target: BigQuery-as-source needs to *carry* the precision so
    # downstream target dialects that do require it can render correctly, even
    # though BigQuery-as-target would never render it itself.
    _BIGQUERY_NUMERIC_PRECISION = 38
    _BIGQUERY_NUMERIC_SCALE = 9
    _BIGQUERY_BIGNUMERIC_PRECISION = 76
    _BIGQUERY_BIGNUMERIC_SCALE = 38

    def extract_numeric_precision(self, row, columns) -> Optional[int]:
        data_type_name: str = self.extract_data_type_name(row, columns).lower()
        if data_type_name == "numeric":
            return self._BIGQUERY_NUMERIC_PRECISION
        if data_type_name == "bignumeric":
            return self._BIGQUERY_BIGNUMERIC_PRECISION
        return super().extract_numeric_precision(row, columns)

    def extract_numeric_scale(self, row, columns) -> Optional[int]:
        data_type_name: str = self.extract_data_type_name(row, columns).lower()
        if data_type_name == "numeric":
            return self._BIGQUERY_NUMERIC_SCALE
        if data_type_name == "bignumeric":
            return self._BIGQUERY_BIGNUMERIC_SCALE
        return super().extract_numeric_scale(row, columns)

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_coerce(self, expr: str) -> str:
        # BigQuery coerces bare string comparison literals to the column's type, and an
        # ISO string with a UTC offset cannot cast to DATETIME. Wrapping the column makes
        # both sides TIMESTAMP-typed.
        return f"timestamp({expr})"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"

    def literal_string(self, value: str) -> Optional[str]:
        if value is None:
            return None
        # BigQuery triple-quoted strings ('''...''') allow multiline content and
        # embedded single quotes without escaping.  The only sequence that needs
        # escaping inside triple-quoted strings is ''' itself and backslashes.
        escaped = value.replace("\\", "\\\\").replace("'''", "\\'''")
        return "'''" + escaped + "'''"

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        # The first select row should have column aliases
        # Remaining rows don't need aliases
        def build_literal_with_alias(literal, alias: COLUMN | None) -> str:
            return f"{self.literal(literal)} AS {self.quote_column(alias.name)}"

        literal_rows: list[str] = []
        for tuple in values.values:
            if alias_columns:
                literal_sqls: list[str] = []
                for i in range(len(tuple.expressions)):
                    literal: LITERAL = tuple.expressions[i]
                    alias: COLUMN = alias_columns[i]
                    literal_sqls.append(build_literal_with_alias(literal=literal, alias=alias))
                literal_rows.append(", ".join(literal_sql for literal_sql in literal_sqls))
                alias_columns = None
            else:
                literal_rows.append(", ".join(self.build_expression_sql(e) for e in tuple.expressions))

        select_rows: list[str] = [f"SELECT {literal_row}" for literal_row in literal_rows]

        return "\nUNION ALL ".join([select_row for select_row in select_rows])

    def _build_cte_with_sql_line(self, with_element: WITH) -> str:
        return f"WITH {self.quote_default(with_element.alias)} AS ("

    def get_max_sql_statement_length(self) -> int:
        # This is a VERY conservative limit. We can probably get away with more
        # However, BigQuery started complaining that the statement was "too complicated" with a length of 1MB.
        return 200 * 1024  # 200KB

    def get_preferred_number_of_rows_for_insert(self) -> int:
        return 500

    def _build_concat_ws_sql(self, concat_ws: CONCAT_WS) -> str:
        elements: str = f", '{concat_ws.separator}', ".join(self.build_expression_sql(e) for e in concat_ws.expressions)
        return f"CONCAT({elements})"

    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        return f"to_hex({super()._build_string_hash_sql(string_hash)})"

    def _build_random_sql(self, random: RANDOM) -> str:
        return "RAND()"
