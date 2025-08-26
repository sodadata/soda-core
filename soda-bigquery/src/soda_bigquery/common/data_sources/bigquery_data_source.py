import logging

from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import (
    COLUMN,
    COUNT,
    DISTINCT,
    LITERAL,
    REGEX_LIKE,
    TUPLE,
    VALUES,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.metadata_types import SodaDataTypeNames
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery

logger: logging.Logger = soda_logger


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel):
        super().__init__(data_source_model=data_source_model)
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

    def create_metadata_columns_query(self) -> MetadataColumnsQuery:
        super_metadata_columns_query = MetadataColumnsQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            prefixes=[f"region-{self.get_location()}"],
        )
        return super_metadata_columns_query


class BigQuerySqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            SodaDataTypeNames.TEXT: "STRING",
            SodaDataTypeNames.INTEGER: "INT64",
            SodaDataTypeNames.DECIMAL: "FLOAT64",
            SodaDataTypeNames.DATE: "DATE",
            SodaDataTypeNames.TIME: "TIME",
            SodaDataTypeNames.TIMESTAMP: "TIMESTAMP",
            SodaDataTypeNames.TIMESTAMP_TZ: "TIMESTAMP",  # BigQuery does not have a separate TZ type; it's always in UTC
            SodaDataTypeNames.BOOLEAN: "BOOL",
        }

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

    def supports_data_type_character_maximun_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"

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
