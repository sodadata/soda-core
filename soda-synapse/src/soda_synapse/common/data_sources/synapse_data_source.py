import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import (
    COLUMN,
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
    INSERT_INTO,
    VALUES,
    VALUES_ROW,
)
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
    def __init__(self, data_source_model: SynapseDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return SynapseSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SynapseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SynapseSqlDialect(SqlServerSqlDialect, sqlglot_dialect="tsql"):
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
        # Synapse uses clustered columnstore indexes by default, which don't support varchar(MAX).
        # Use HEAP to create a heap table instead.
        create_table_sql = create_table_sql + "\nWITH (HEAP)"
        return create_table_sql + (";" if add_semicolon else "")

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETIMEFROMPARTS((datepart(YEAR, {timestamp_literal})), (datepart(MONTH, {timestamp_literal})), (datepart(DAY, {timestamp_literal})), 0, 0, 0, 0)"

    def _build_insert_into_values_sql(self, insert_into: INSERT_INTO) -> str:
        values_sql: str = "\n" + "\nUNION ALL ".join(
            [self._build_insert_into_values_row_sql(value) for value in insert_into.values]
        )
        return values_sql

    def _build_insert_into_values_row_sql(self, values: VALUES_ROW) -> str:
        values_sql: str = "SELECT " + ", ".join([self.literal(value) for value in values.values])
        return values_sql

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
        # Synapse Dedicated SQL Pool does not support OFFSET ... FETCH NEXT, so we paginate via
        # ROW_NUMBER() over the key columns and INNER JOIN back to the original table. Joining
        # rather than wrapping in a CTE keeps the row-shape clean (no synthetic rn column leaks
        # into the result set, which would otherwise be mistaken for a real column by the
        # downstream rows_diff comparator).
        qualified_table = self.build_fully_qualified_sql_name(dataset_identifier)
        quoted_keys = [self.quote_default(c) for c in order_by]
        keys_csv = ", ".join(quoted_keys)
        order_by_csv = ", ".join(f"{c} ASC" for c in quoted_keys)
        join_condition = " AND ".join(f"t.{c} = p.{c}" for c in quoted_keys)
        where_sql = f"WHERE {filter}" if filter else ""

        if columns:
            select_clause = "SELECT " + ", ".join(f"t.{self.quote_default(c)}" for c in columns)
        else:
            select_clause = "SELECT t.*"

        return (
            f"WITH paginated_keys AS (\n"
            f"    SELECT {keys_csv}, ROW_NUMBER() OVER (ORDER BY {order_by_csv}) AS rn\n"
            f"    FROM {qualified_table}\n"
            f"    {where_sql}\n"
            f")\n"
            f"{select_clause}\n"
            f"FROM {qualified_table} AS t\n"
            f"INNER JOIN paginated_keys AS p ON {join_condition}\n"
            f"WHERE p.rn > {offset} AND p.rn <= {offset + limit}\n"
            f"ORDER BY p.rn;"
        )
