import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import COLUMN, INSERT_INTO, TUPLE, VALUES, VALUES_ROW
from soda_core.common.sql_dialect import SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source import (
    SqlServerDataSourceImpl,
    SqlServerSqlDialect,
)
from soda_synapse.common.data_sources.synapse_data_source_connection import (
    SynapseDataSource as SynapseDataSourceModel,
)
from soda_synapse.common.data_sources.synapse_data_source_connection import (
    SynapseDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SynapseDataSourceImpl(SqlServerDataSourceImpl, model_class=SynapseDataSourceModel):
    def __init__(self, data_source_model: SynapseDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SynapseSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SynapseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SynapseSqlDialect(SqlServerSqlDialect):
    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETIMEFROMPARTS((datepart(YEAR, {timestamp_literal})), (datepart(MONTH, {timestamp_literal})), (datepart(DAY, {timestamp_literal})), 0, 0, 0, 0)"

    def _build_insert_into_values_sql(self, insert_into: INSERT_INTO) -> str:
        values_sql: str = "\n" + "\nUNION ALL ".join(
            [self._build_insert_into_values_row_sql(value) for value in insert_into.values]
        )
        return values_sql

    def _build_insert_into_values_row_sql(self, values: VALUES_ROW) -> str:
        values_sql: str = "SELECT " + ", ".join([self.literal(value) for value in values.values])
        values_sql = self.encode_string_for_sql(values_sql)
        return values_sql

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return "\nUNION ALL\n".join(["SELECT " + self.build_expression_sql(value) for value in values.values])

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        elements: str = ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
        if tuple.check_context(VALUES):
            # in built_cte_values_sql, elements are dropped in top-level select statement, so can't use parentheses
            return elements
        return f"({elements})"

    def select_all_paginated_sql(
        self,
        dataset_identifier: DatasetIdentifier,
        columns: list[str],
        filter: Optional[str],
        order_by: list[str],
        limit: int,
        offset: int,
    ) -> str:
        """TODO: Synapse uses completely different pagination syntax, AST does not have enough flexibility for this yet."""
        where_clauses = []

        if filter:
            where_clauses.append(SqlExpressionStr(filter))

        select_statements = self._build_select_sql_lines([SELECT(columns or [STAR()])])
        select = ", ".join(select_statements)
        order_by_statements = self._build_order_by_lines([ORDER_BY_ASC(c) for c in order_by])
        order_by = ", ".join(order_by_statements)
        where_statements = self._build_where_sql_lines([WHERE.optional(AND.optional(where_clauses))])
        where = ", ".join(where_statements)

        query = f"""WITH src AS (
        {select}, ROW_NUMBER()
            OVER (
                {order_by}
            ) AS rn
        FROM {self.build_fully_qualified_sql_name(dataset_identifier)} AS t
        {where}
        )
        SELECT *
            FROM src
            WHERE rn > {offset}
            AND rn <= {offset + limit}
        ORDER BY rn;
        """

        return query
