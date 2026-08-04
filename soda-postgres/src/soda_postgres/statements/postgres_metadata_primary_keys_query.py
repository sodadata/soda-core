from __future__ import annotations

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


class PostgresMetadataPrimaryKeysQuery(MetadataPrimaryKeysQuery):
    """Bulk primary-key introspection for Postgres, reading pg_catalog instead of
    information_schema.

    Postgres filters information_schema.table_constraints / key_column_usage to tables the
    current user owns or holds a non-SELECT privilege on, so a read-only monitoring user
    (the common production grant) silently gets zero primary keys. The pg_catalog views are
    not privilege-filtered — same reason PostgresMetadataTablesQuery bypasses
    information_schema.

    The primary key is one pg_constraint row (contype = 'p') whose conkey array lists the
    key columns' attnums in key order; joining pg_attribute on conkey membership and
    selecting array_position(conkey, attnum) yields the same
    (table_name, column_name, ordinal_position) rows the base query produces, so execute()
    and get_results() are inherited unchanged.
    """

    def build_sql_statement(self, table_namespace: DataSourceNamespace, table_names: list[str]) -> list:
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()
        pg_catalog: str = self.sql_dialect._pg_catalog()

        return [
            SELECT(
                [
                    COLUMN("relname", "tables"),
                    COLUMN("attname", "key_columns"),
                    RAW_SQL("array_position(constraints.conkey, key_columns.attnum)"),
                ]
            ),
            FROM("pg_constraint", table_prefix=[pg_catalog], alias="constraints"),
            JOIN(
                table_name=self.sql_dialect._pg_class(),
                table_prefix=[pg_catalog],
                alias="tables",
                on_condition=EQ(COLUMN("oid", "tables"), COLUMN("conrelid", "constraints")),
            ),
            JOIN(
                table_name=self.sql_dialect._pg_namespace(),
                table_prefix=[pg_catalog],
                alias="schemas",
                on_condition=EQ(COLUMN("oid", "schemas"), COLUMN("relnamespace", "tables")),
            ),
            JOIN(
                table_name="pg_attribute",
                table_prefix=[pg_catalog],
                alias="key_columns",
                on_condition=AND(
                    [
                        EQ(COLUMN("attrelid", "key_columns"), COLUMN("conrelid", "constraints")),
                        RAW_SQL("key_columns.attnum = ANY(constraints.conkey)"),
                    ]
                ),
            ),
            WHERE(
                AND(
                    [
                        EQ(COLUMN("contype", "constraints"), LITERAL("p")),
                        *(
                            # Postgres can only introspect the connected database; mirror
                            # PostgresMetadataTablesQuery's case-insensitive guard.
                            [EQ(LOWER(RAW_SQL(self.sql_dialect._current_database())), LITERAL(database_name.lower()))]
                            if database_name
                            else []
                        ),
                        # Case-insensitive like PostgresMetadataTablesQuery, so a schema spelled in a
                        # different case in the contract doesn't silently match nothing.
                        EQ(LOWER(COLUMN("nspname", "schemas")), LITERAL(schema_name.lower())),
                        IN(
                            COLUMN("relname", "tables"),
                            [LITERAL(self.sql_dialect.metadata_casify(name)) for name in table_names],
                        ),
                    ]
                )
            ),
        ]
