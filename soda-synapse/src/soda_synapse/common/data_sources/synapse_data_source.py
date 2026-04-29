import logging
from typing import Callable, Optional

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
        # Inject a column-name resolver so the ROW_NUMBER paginator can fall back to the
        # table's column list when the caller didn't supply an explicit `columns`. The
        # callable pattern (as used by Athena's `get_table_storage_location`) keeps the
        # dialect decoupled from `DataSourceImpl` per PR #2600.
        return SynapseSqlDialect(get_column_names=self._get_column_names_for_pagination)

    def _get_column_names_for_pagination(self, dataset_prefixes: list[str], dataset_name: str) -> list[str]:
        return [
            cm.column_name
            for cm in self.get_columns_metadata(dataset_prefixes=dataset_prefixes, dataset_name=dataset_name)
        ]

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SynapseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SynapseSqlDialect(SqlServerSqlDialect, sqlglot_dialect="tsql"):
    def __init__(self, get_column_names: Optional[Callable[[list[str], str], list[str]]] = None):
        super().__init__()
        # Optional callable that resolves a table's column names from `(prefixes, name)`. Used
        # by `select_all_paginated_sql` when the caller didn't supply an explicit `columns`.
        # Mirrors Athena's `get_table_storage_location` injection pattern from PR #2600 — the
        # dialect stays decoupled from `DataSourceImpl` and just has a function it can call.
        # Optional so the dialect can still be constructed in tests / contexts that don't
        # paginate or always pass explicit columns.
        self._get_column_names: Optional[Callable[[list[str], str], list[str]]] = get_column_names
        # Cache of resolved column lists keyed by (prefixes_tuple, dataset_name) so we don't
        # issue a metadata round-trip per page during a paginated scan.
        self._columns_cache: dict[tuple[tuple[str, ...], str], list[str]] = {}

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

    def _quote_identifier_safe(self, identifier: str) -> str:
        """T-SQL-safe identifier quoting that escapes closing brackets.

        `quote_default` (inherited from SqlServerSqlDialect) wraps in `[...]` without escaping
        any `]` characters embedded in the identifier — fine in practice for trusted
        identifiers, but allows SQL injection if the identifier is not pre-validated. The
        Synapse paginator below interpolates column names directly into a hand-built SQL
        string (rather than going through the AST), so it must defend against this. The T-SQL
        rule for bracket-quoted identifiers is to double any embedded `]`.
        """
        return f"[{identifier.replace(']', ']]')}]"

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
        # ROW_NUMBER(). The earlier implementation joined `t.<key> = p.<key>` to drop the rn
        # column from the result set, but `NULL = NULL` is false and duplicate keys amplify
        # rows past the page size — both diverge from native OFFSET/LIMIT semantics on other
        # data sources and break the rows_diff merge join. Instead we compute rn inside an
        # inner CTE alongside the requested columns and select only those columns from the
        # outer SELECT, so rn never leaks and no JOIN is needed.
        #
        # We need an explicit column list to project rn out. When the caller doesn't supply
        # one (e.g. rows_diff with no `source_columns`/`target_columns` in YAML), the dialect
        # falls back to the injected `get_column_names` callable. Other dialects don't need
        # this because they paginate with native OFFSET/LIMIT and `SELECT *` works fine.
        if not columns:
            assert self._get_column_names is not None, (
                "SynapseSqlDialect needs `get_column_names` to be supplied at construction "
                "(via `SynapseDataSourceImpl._create_sql_dialect`) or an explicit `columns` list."
            )
            cache_key = (tuple(dataset_identifier.prefixes), dataset_identifier.dataset_name)
            cached = self._columns_cache.get(cache_key)
            if cached is None:
                cached = self._get_column_names(dataset_identifier.prefixes, dataset_identifier.dataset_name)
                self._columns_cache[cache_key] = cached
            columns = cached
        qualified_table = self.build_fully_qualified_sql_name(dataset_identifier)
        # Use the safe quoter so column / order-by identifiers can't break out of `[...]`.
        quoted_columns = [self._quote_identifier_safe(c) for c in columns]
        columns_csv = ", ".join(quoted_columns)
        # An empty `order_by` is allowed by the base `SqlDialect.select_all_paginated_sql`
        # contract — produce a deterministic-enough fallback that T-SQL accepts inside the
        # OVER(...) clause. `(SELECT NULL)` is the standard idiom for "any order".
        order_by_csv = (
            ", ".join(f"{self._quote_identifier_safe(c)} ASC" for c in order_by) if order_by else "(SELECT NULL)"
        )
        where_sql = f"WHERE {filter}" if filter else ""
        # Use a `__soda_`-prefixed alias so we don't collide with a real column named `rn`
        # (possible when `columns` was resolved from the table's metadata).
        rn_alias = "__soda_rn"

        return (
            f"WITH paginated AS (\n"
            f"    SELECT {columns_csv}, ROW_NUMBER() OVER (ORDER BY {order_by_csv}) AS {rn_alias}\n"
            f"    FROM {qualified_table}\n"
            f"    {where_sql}\n"
            f")\n"
            f"SELECT {columns_csv}\n"
            f"FROM paginated\n"
            f"WHERE {rn_alias} > {offset} AND {rn_alias} <= {offset + limit}\n"
            f"ORDER BY {rn_alias};"
        )
