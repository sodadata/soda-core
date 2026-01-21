from __future__ import annotations

from typing import Optional

from soda_core.common.sql_ast import (
    COLUMN,
    EQ,
    FROM,
    IN,
    JOIN,
    LIKE,
    LITERAL,
    LOWER,
    NOT_LIKE,
    OR,
    RAW_SQL,
    SELECT,
    WHERE,
)
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery


class PostgresMetadataTablesQuery(MetadataTablesQuery):
    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list:
        """
        Builds the full SQL query statement to query table names from the data source metadata.
        """
        current_database_expression = RAW_SQL(self.sql_dialect.current_database())
        select: list = [
            SELECT(
                [
                    COLUMN(current_database_expression, field_alias="table_catalog"),
                    COLUMN("nspname", table_alias="n", field_alias="table_schema"),
                    COLUMN("relname", table_alias="c", field_alias="table_name"),
                    RAW_SQL(self.sql_dialect.relkind_table_type_sql_expression()),
                ]
            ),
            FROM(
                self.sql_dialect.pg_class(),
                table_prefix=[self.sql_dialect.pg_catalog()],
                alias="c",
            ),
            JOIN(
                table_name=self.sql_dialect.pg_namespace(),
                table_prefix=[self.sql_dialect.pg_catalog()],
                alias="n",
                on_condition=EQ(
                    COLUMN("relnamespace", "c"),
                    COLUMN("oid", "n"),
                ),
            ),
            # Only get object types that correspond to tables/views in information_schema.tables
            WHERE(IN(COLUMN("relkind", "c"), [LITERAL("r"), LITERAL("p"), LITERAL("v"), LITERAL("m"), LITERAL("f")])),
        ]

        if database_name:
            database_name_lower: str = database_name.lower()
            select.append(WHERE(EQ(LOWER(current_database_expression), LITERAL(database_name_lower))))

        if schema_name:
            select.append(WHERE(EQ(LOWER(COLUMN("nspname", "n")), LITERAL(schema_name.lower()))))

        if include_table_name_like_filters:
            select.append(
                WHERE(
                    OR(
                        [
                            LIKE(LOWER(COLUMN("relname", "c")), LITERAL(include_table_name_like_filter.lower()))
                            for include_table_name_like_filter in include_table_name_like_filters
                        ]
                    )
                )
            )

        if exclude_table_name_like_filters:
            for exclude_table_name_like_filter in exclude_table_name_like_filters:
                select.append(
                    WHERE(NOT_LIKE(LOWER(COLUMN("relname", "c")), LITERAL(exclude_table_name_like_filter.lower())))
                )

        return select
