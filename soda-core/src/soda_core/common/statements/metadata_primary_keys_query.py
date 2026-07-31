from __future__ import annotations

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import (
    DataSourceNamespace,
    DbSchemaDataSourceNamespace,
    SchemaDataSourceNamespace,
)
from soda_core.common.sql_ast import (
    AND,
    COLUMN,
    EQ,
    FROM,
    IN,
    JOIN,
    LITERAL,
    SELECT,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect


class MetadataPrimaryKeysQuery:
    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection,
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection

    def execute(self, dataset_prefixes: list[str], dataset_names: list[str]) -> dict[str, set[str]]:
        """Fetches the primary key column names for the given tables in a single query,
        keyed by table name."""
        if not dataset_names:
            return {}
        table_namespace: DataSourceNamespace = self._build_namespace(dataset_prefixes)
        select_statement: list = self.build_sql_statement(table_namespace=table_namespace, table_names=dataset_names)
        sql: str = self.sql_dialect.build_select_sql(select_statement)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result)

    def _build_namespace(self, prefixes: list[str]) -> DataSourceNamespace:
        schema_name: str | None = self._extract_from_prefix(prefixes, self.sql_dialect.get_schema_prefix_index())
        database_name: str | None = self._extract_from_prefix(prefixes, self.sql_dialect.get_database_prefix_index())
        return (
            SchemaDataSourceNamespace(schema=schema_name)
            if database_name is None
            else DbSchemaDataSourceNamespace(database=database_name, schema=schema_name)
        )

    @staticmethod
    def _extract_from_prefix(prefixes: list[str], index: int | None) -> str | None:
        if index is None:
            return None
        return prefixes[index] if index < len(prefixes) else None

    ########################################################
    # Constraint-specific tokens (override for non-standard data sources)
    ########################################################

    def table_table_constraints(self) -> str:
        """
        Name of the information_schema view holding table constraint metadata.
        Purpose of this method is to allow specific data sources to override.
        """
        return self.sql_dialect.default_casify("table_constraints")

    def table_key_column_usage(self) -> str:
        """
        Name of the information_schema view holding the columns participating in constraints.
        Purpose of this method is to allow specific data sources to override.
        """
        return self.sql_dialect.default_casify("key_column_usage")

    def column_constraint_name(self) -> str:
        return self.sql_dialect.default_casify("constraint_name")

    def column_constraint_type(self) -> str:
        return self.sql_dialect.default_casify("constraint_type")

    def primary_key_constraint_type_value(self) -> str:
        """
        The value stored in information_schema.table_constraints.constraint_type for a primary key.
        """
        return "PRIMARY KEY"

    ########################################################

    def build_sql_statement(self, table_namespace: DataSourceNamespace, table_names: list[str]) -> list:
        """
        Builds the SQL statement returning, per table, the table name and its primary key column
        names for the given tables, using the standard information_schema.table_constraints /
        information_schema.key_column_usage approach. The table name is selected alongside the
        column name so results can be grouped by table.

        Purpose of this method is to allow specific data sources to override.
        """
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()
        information_schema = self.sql_dialect.information_schema_namespace_elements(table_namespace)

        return [
            SELECT(
                [
                    COLUMN(self.sql_dialect.column_table_name(), "tc"),
                    COLUMN(self.sql_dialect.column_column_name(), "kcu"),
                ]
            ),
            FROM(self.table_table_constraints(), alias="tc").IN(information_schema),
            JOIN(
                table_name=self.table_key_column_usage(),
                table_prefix=information_schema,
                alias="kcu",
                on_condition=EQ(
                    COLUMN(self.column_constraint_name(), "tc"),
                    COLUMN(self.column_constraint_name(), "kcu"),
                ),
            ),
            WHERE(
                AND(
                    [
                        EQ(
                            COLUMN(self.column_constraint_type(), "tc"),
                            LITERAL(self.primary_key_constraint_type_value()),
                        ),
                        *(
                            [
                                EQ(
                                    COLUMN(self.sql_dialect.column_table_catalog(), "tc"),
                                    LITERAL(self.sql_dialect.metadata_casify(database_name)),
                                )
                            ]
                            if database_name
                            else []
                        ),
                        EQ(
                            COLUMN(self.sql_dialect.column_table_schema(), "tc"),
                            LITERAL(self.sql_dialect.metadata_casify(schema_name)),
                        ),
                        IN(
                            COLUMN(self.sql_dialect.column_table_name(), "tc"),
                            [LITERAL(self.sql_dialect.metadata_casify(name)) for name in table_names],
                        ),
                    ]
                )
            ),
        ]

    def get_results(self, query_result: QueryResult) -> dict[str, set[str]]:
        """
        Groups the query result rows into {table_name: {primary_key_column_names}}.
        The first selected column is the table name and the second is the primary key column name.
        Rows with a null table name or column name are skipped.
        """
        primary_keys_by_table: dict[str, set[str]] = {}
        for row in query_result.rows:
            if not row or row[0] is None or row[1] is None:
                continue
            table_name, column_name = row[0], row[1]
            primary_keys_by_table.setdefault(table_name, set()).add(column_name)
        return primary_keys_by_table
