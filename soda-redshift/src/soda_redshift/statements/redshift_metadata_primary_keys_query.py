from __future__ import annotations

from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import DataSourceNamespace
from soda_core.common.sql_ast import (
    AND,
    COLUMN,
    EQ,
    FROM,
    IN,
    JOIN,
    LITERAL,
    LOWER,
    RAW_SQL,
    SELECT,
    WHERE,
)
from soda_core.common.statements.metadata_primary_keys_query import (
    MetadataPrimaryKeysQuery,
)


class RedshiftMetadataPrimaryKeysQuery(MetadataPrimaryKeysQuery):
    """Bulk primary-key introspection for Redshift, reading pg_catalog instead of
    information_schema.

    Redshift's information_schema has Postgres-8 lineage: its constraint views only list
    tables the current user owns, so a read-only monitoring user silently gets zero primary
    keys (same reason table_schemata reads SVV_ALL_SCHEMAS — see SCS-1193 — and the tables /
    columns queries read svv_tables / svv_columns). There is no SVV constraints view, so the
    primary key is read from pg_catalog.pg_constraint (contype = 'p').

    Redshift has no array_position(), so the postgres pg_catalog query cannot be reused to
    order the key columns. Instead pg_get_constraintdef(oid) — the same function AWS's
    v_generate_tbl_ddl admin view uses — renders the constraint as
    ``PRIMARY KEY (col1, col2, ...)`` in declared key order, and get_results parses the
    column list out of it.
    """

    def build_sql_statement(self, table_namespace: DataSourceNamespace, table_names: list[str]) -> list:
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()

        return [
            SELECT(
                [
                    COLUMN("relname", "tables"),
                    RAW_SQL("pg_get_constraintdef(constraints.oid)"),
                ]
            ),
            FROM("pg_constraint", table_prefix=["pg_catalog"], alias="constraints"),
            JOIN(
                table_name="pg_class",
                table_prefix=["pg_catalog"],
                alias="tables",
                on_condition=EQ(COLUMN("oid", "tables"), COLUMN("conrelid", "constraints")),
            ),
            JOIN(
                table_name="pg_namespace",
                table_prefix=["pg_catalog"],
                alias="schemas",
                on_condition=EQ(COLUMN("oid", "schemas"), COLUMN("relnamespace", "tables")),
            ),
            WHERE(
                AND(
                    [
                        EQ(COLUMN("contype", "constraints"), LITERAL("p")),
                        *(
                            # Redshift can only introspect the connected database; mirror the
                            # postgres query's case-insensitive guard.
                            [EQ(LOWER(RAW_SQL("current_database()")), LITERAL(database_name.lower()))]
                            if database_name
                            else []
                        ),
                        # Case-insensitive like the postgres query, so a schema spelled in a
                        # different case doesn't silently match nothing.
                        EQ(LOWER(COLUMN("nspname", "schemas")), LITERAL(schema_name.lower())),
                        IN(
                            COLUMN("relname", "tables"),
                            [LITERAL(self.sql_dialect.metadata_casify(name)) for name in table_names],
                        ),
                    ]
                )
            ),
        ]

    def get_results(self, query_result: QueryResult) -> dict[str, list[str]]:
        """Groups rows into {table_name: [primary_key_column_names]} in declared key order.
        Each row is (table_name, constraint_definition); a table has at most one primary key,
        so one row per table. Rows with a null table name or definition are skipped.
        """
        primary_keys_by_table: dict[str, list[str]] = {}
        for row in query_result.rows:
            if not row or row[0] is None or row[1] is None:
                continue
            table_name, constraint_definition = row[0], row[1]
            primary_keys_by_table[table_name] = self._parse_primary_key_columns(constraint_definition)
        return primary_keys_by_table

    @staticmethod
    def _parse_primary_key_columns(constraint_definition: str) -> list[str]:
        """Parses the column list out of ``PRIMARY KEY (col1, "Quoted, col", ...)``,
        preserving order. Splits on commas outside double quotes; strips the quotes and
        un-doubles embedded ``""`` escapes.
        """
        inner: str = constraint_definition[constraint_definition.index("(") + 1 : constraint_definition.rindex(")")]
        columns: list[str] = []
        current: list[str] = []
        in_quotes = False
        index = 0
        while index < len(inner):
            char = inner[index]
            if char == '"':
                if in_quotes and index + 1 < len(inner) and inner[index + 1] == '"':
                    current.append('"')
                    index += 2
                    continue
                in_quotes = not in_quotes
                index += 1
                continue
            if char == "," and not in_quotes:
                columns.append("".join(current).strip())
                current = []
                index += 1
                continue
            current.append(char)
            index += 1
        if current:
            columns.append("".join(current).strip())
        return columns
