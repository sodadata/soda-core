import logging

from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSourceConnection,
)
from soda_bigquery.common.statements.metadata_columns_query import (
    BigQueryMetadataColumnsQuery,
)
from soda_bigquery.common.statements.metadata_tables_query import (
    BigQueryMetadataTablesQuery,
)
from soda_bigquery.model.data_source.bigquery_data_source import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import DISTINCT, REGEX_LIKE, TUPLE
from soda_core.common.sql_dialect import SqlDialect

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
        location = self.data_source_model.connection_properties.location
        if location is None:
            if self.cached_location is not None:
                location = self.cached_location
            else:
                result = self.execute_query("SELECT @@location")
                location = result.rows[0][0]
                logger.info(f"Detected BigQuery location: {location}")
                self.cached_location = location
        return location

    def create_metadata_tables_query(self) -> BigQueryMetadataTablesQuery:
        super_metadata_tables_query = BigQueryMetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            location=self.get_location(),
        )
        return super_metadata_tables_query

    def create_metadata_columns_query(self) -> BigQueryMetadataColumnsQuery:
        super_metadata_columns_query = BigQueryMetadataColumnsQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            location=self.get_location(),
        )
        return super_metadata_columns_query


class BigQuerySqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"[{super()._build_tuple_sql(tuple)}]"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"TO_JSON_STRING(STRUCT({super()._build_tuple_sql(tuple)}))"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_CONTAINS({expression}, r'{matches.regex_pattern}')"

    def supports_varchar_length(self) -> bool:
        return False

    def quote_column(self, column_name: str) -> str:
        return self.quote_default(column_name)

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"
