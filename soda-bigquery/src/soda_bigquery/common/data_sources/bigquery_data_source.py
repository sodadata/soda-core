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
from soda_core.common.sql_ast import (
    CASE_WHEN,
    DISTINCT,
    REGEX_LIKE,
    TUPLE,
    SqlExpression,
)
from soda_core.common.sql_dialect import SqlDialect


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return BigQuerySqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return BigQueryDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def get_location(self) -> str:
        return self.data_source_model.connection_properties.location

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
        return f"[{super()._build_tuple_sql(tuple)}]"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"TO_JSON_STRING(STRUCT({super()._build_tuple_sql(tuple)}))"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_CONTAINS({expression}, r'{matches.regex_pattern}')"

    def _build_case_when_sql(self, case_when: CASE_WHEN, in_distinct: bool = False) -> str:
        if_expression_statement = None
        else_expression_statement = None
        if in_distinct and isinstance(case_when.if_expression, TUPLE):
            if_expression_statement = self._build_tuple_sql_in_distinct(case_when.if_expression)
        else:
            if_expression_statement = self.build_expression_sql(case_when.if_expression)

        if in_distinct and isinstance(case_when.else_expression, TUPLE):
            else_expression_statement = self._build_tuple_sql_in_distinct(case_when.else_expression)
        else:
            else_expression_statement = (
                self.build_expression_sql(case_when.else_expression) if case_when.else_expression else None
            )

        return (
            f"CASE WHEN {self.build_expression_sql(case_when.condition)} "
            + f"THEN {if_expression_statement} "
            + (f"ELSE {else_expression_statement} " if else_expression_statement else "")
            + "END"
        )

    def _build_distinct_sql(self, distinct: DISTINCT) -> str:
        expressions: list[SqlExpression] = (
            distinct.expression if isinstance(distinct.expression, list) else [distinct.expression]
        )
        result_list = []
        for e in expressions:
            if isinstance(
                e, TUPLE
            ):  # BigQuery does not support DISTINCT on multiple columns, so we have to trick it using a TO_JSON_STRING
                result_list.append(self._build_tuple_sql_in_distinct(e))
            elif isinstance(e, CASE_WHEN):
                result_list.append(self._build_case_when_sql(e, in_distinct=True))
            else:
                result_list.append(self.build_expression_sql(e))

        result = ", ".join(result_list)
        return f"DISTINCT({result})"

    def supports_varchar_length(self) -> bool:
        return False
