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

    def execute(self, dataset_prefixes: list[str], dataset_names: list[str]) -> dict[str, list[str]]:
        """Fetches the primary key column names for the given tables in a single query, keyed by
        table name, each list ordered by the column's position within the primary key."""
        if not dataset_names:
            return {}
        table_namespace: DataSourceNamespace = self._build_namespace(dataset_prefixes)
        select_statement: list = self.build_sql_statement(table_namespace=table_namespace, table_names=dataset_names)
        sql: str = self.sql_dialect.build_select_sql(select_statement)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result)

    def _build_namespace(self, prefixes: list[str]) -> DataSourceNamespace:
        schema_index: int | None = self.sql_dialect.get_schema_prefix_index()
        schema_name: str | None = self._extract_from_prefix(prefixes, schema_index)
        # A dialect that declares a schema prefix index REQUIRES a schema to scope the metadata
        # query. If the prefix list is too short to hold it, _extract_from_prefix returns None and
        # the query would emit `table_schema = NULL` (matching nothing, silently returning no PKs).
        # Fail loud instead so the misconfiguration surfaces rather than looking like "no PKs".
        # The database is not subject to this check: schema-only dialects legitimately have no
        # database, and db+schema dialects tolerate a missing database in the filter.
        if schema_index is not None and schema_name is None:
            raise ValueError(
                f"Cannot resolve a schema from dataset prefixes {prefixes!r}: this data source "
                f"requires a schema at prefix index {schema_index}, but the prefixes are too short."
            )
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

    def column_ordinal_position(self) -> str:
        """Column in key_column_usage giving each column's 1-based position within the constraint;
        used to return the primary key columns in their composite-key order."""
        return self.sql_dialect.default_casify("ordinal_position")

    def primary_key_constraint_type_value(self) -> str:
        """
        The value stored in information_schema.table_constraints.constraint_type for a primary key.
        """
        return "PRIMARY KEY"

    ########################################################

    def build_sql_statement(self, table_namespace: DataSourceNamespace, table_names: list[str]) -> list:
        """
        Builds the SQL statement returning, per table, the table name, each primary key column name,
        and that column's ordinal position within the primary key, for the given tables, using the
        standard information_schema.table_constraints / information_schema.key_column_usage approach.
        The table name is selected alongside the column name so results can be grouped by table, and
        the ordinal position so get_results can order composite keys.

        Purpose of this method is to allow specific data sources to override.
        """
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()
        information_schema = self.sql_dialect.information_schema_namespace_elements(table_namespace)

        return [
            SELECT(
                [
                    COLUMN(self.sql_dialect.column_table_name(), "constraints"),
                    COLUMN(self.sql_dialect.column_column_name(), "key_columns"),
                    COLUMN(self.column_ordinal_position(), "key_columns"),
                ]
            ),
            FROM(self.table_table_constraints(), alias="constraints").IN(information_schema),
            JOIN(
                table_name=self.table_key_column_usage(),
                table_prefix=information_schema,
                alias="key_columns",
                # constraint_name is unique only within (constraint_catalog, constraint_schema),
                # NOT globally. Joining on constraint_name alone lets key_column_usage rows from a
                # DIFFERENT schema/table whose PK constraint happens to share the same name (e.g.
                # Postgres auto-names every PK "<table>_pkey"; MySQL names every PK "PRIMARY") join
                # in and leak phantom PK columns onto the target table. Match schema + table (and
                # catalog when present) as well so the join stays within one constraint namespace.
                # Do not "simplify" this back to constraint_name only.
                on_condition=AND(
                    [
                        EQ(
                            COLUMN(self.column_constraint_name(), "constraints"),
                            COLUMN(self.column_constraint_name(), "key_columns"),
                        ),
                        EQ(
                            COLUMN(self.sql_dialect.column_table_schema(), "constraints"),
                            COLUMN(self.sql_dialect.column_table_schema(), "key_columns"),
                        ),
                        EQ(
                            COLUMN(self.sql_dialect.column_table_name(), "constraints"),
                            COLUMN(self.sql_dialect.column_table_name(), "key_columns"),
                        ),
                        *(
                            [
                                EQ(
                                    COLUMN(self.sql_dialect.column_table_catalog(), "constraints"),
                                    COLUMN(self.sql_dialect.column_table_catalog(), "key_columns"),
                                )
                            ]
                            if database_name
                            else []
                        ),
                    ]
                ),
            ),
            WHERE(
                AND(
                    [
                        EQ(
                            COLUMN(self.column_constraint_type(), "constraints"),
                            LITERAL(self.primary_key_constraint_type_value()),
                        ),
                        *(
                            [
                                EQ(
                                    COLUMN(self.sql_dialect.column_table_catalog(), "constraints"),
                                    LITERAL(self.sql_dialect.metadata_casify(database_name)),
                                )
                            ]
                            if database_name
                            else []
                        ),
                        EQ(
                            COLUMN(self.sql_dialect.column_table_schema(), "constraints"),
                            LITERAL(self.sql_dialect.metadata_casify(schema_name)),
                        ),
                        IN(
                            COLUMN(self.sql_dialect.column_table_name(), "constraints"),
                            [LITERAL(self.sql_dialect.metadata_casify(name)) for name in table_names],
                        ),
                    ]
                )
            ),
        ]

    def get_results(self, query_result: QueryResult) -> dict[str, list[str]]:
        """
        Groups the query result rows into {table_name: [primary_key_column_names]}, ordered by each
        column's position within the primary key so composite keys keep their declared order. The
        selected columns are, in order, the table name, the primary key column name, and the ordinal
        position within the constraint. Rows with a null table name or column name are skipped.
        """
        ordered_columns_by_table: dict[str, list[tuple[int, str]]] = {}
        for row in query_result.rows:
            if not row or row[0] is None or row[1] is None:
                continue
            table_name, column_name, ordinal_position = row[0], row[1], row[2]
            ordered_columns_by_table.setdefault(table_name, []).append((int(ordinal_position), column_name))
        return {
            table_name: [column_name for _, column_name in sorted(ordered_columns)]
            for table_name, ordered_columns in ordered_columns_by_table.items()
        }
