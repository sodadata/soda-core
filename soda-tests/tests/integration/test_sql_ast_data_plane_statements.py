"""The data-plane statement nodes and expressions executed end to end, on every dialect that renders
them. A node the dialect refuses with UnsupportedSqlStatementError is left out of the assertion."""

import json
from datetime import datetime

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import UnsupportedSqlStatementError
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    ABS,
    ANALYZE_TABLE,
    AND,
    ARITHMETIC,
    ASSIGNMENT,
    CAST,
    COLUMN,
    CREATE_INDEX_IF_NOT_EXISTS,
    CREATE_SCHEMA_IF_NOT_EXISTS,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CURRENT_TIMESTAMP,
    DATE_ISO_TEXT,
    DATE_TRUNC,
    DELETE,
    DROP_TABLE_IF_EXISTS,
    EPOCH_SECONDS,
    EQ,
    FROM,
    GT,
    INSERT_INTO,
    JOIN,
    JSON_MERGE,
    LIKE,
    LITERAL,
    NULLIF,
    ORDER_BY_ASC,
    REGEXP_REPLACE,
    SELECT,
    SOURCE_COLUMN,
    TIMESTAMP_ISO_TEXT,
    TO_TIMEZONE,
    UPDATE,
    UPSERT,
    UPSERT_VIA_SELECT,
    VALUES_ROW,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect

TARGET = "sqlast_dp_target"
SOURCE = "sqlast_dp_source"
INDEX = "sqlast_dp_target_name_idx"
FIXES = "sqlast_dp_fixes"

# The type a JSON object column is created with; a dialect opting in adds its own.
JSON_TYPE_NAME_BY_DATA_SOURCE_TYPE = {"postgres": "jsonb", "sqlserver": "nvarchar(max)"}


def test_data_plane_statements_end_to_end(data_source_test_helper: DataSourceTestHelper):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    if not sql_dialect.SUPPORTS_DATA_PLANE_STATEMENTS:
        pytest.skip(f"{type(sql_dialect).__name__} does not render the data-plane statements")

    dataset_prefixes: list[str] = data_source_test_helper.dataset_prefix
    target_table: str = sql_dialect.qualify_dataset_name(dataset_prefixes, TARGET)
    source_table: str = sql_dialect.qualify_dataset_name(dataset_prefixes, SOURCE)

    def type_name(soda_type: SodaDataTypeName) -> str:
        return sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()[soda_type]

    def drop_tables() -> None:
        for table in [target_table, source_table]:
            data_source_impl.execute_update(
                sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=table))
            )

    def target_rows() -> list[tuple]:
        return data_source_impl.execute_query(
            sql_dialect.build_select_sql(
                [
                    SELECT([COLUMN("id"), COLUMN("name"), COLUMN("n")]),
                    FROM(TARGET, table_prefix=dataset_prefixes),
                    ORDER_BY_ASC(COLUMN("id")),
                ]
            )
        ).rows

    def upsert(rows: list[tuple], update_assignments: list[ASSIGNMENT] | None) -> int:
        return data_source_impl.execute_update(
            sql_dialect.build_upsert_sql(
                UPSERT(
                    fully_qualified_table_name=target_table,
                    columns=[COLUMN("id"), COLUMN("name"), COLUMN("n")],
                    values=[VALUES_ROW([LITERAL(value) for value in row]) for row in rows],
                    key_columns=[COLUMN("id")],
                    update_assignments=update_assignments,
                )
            )
        )

    drop_tables()
    try:
        data_source_impl.execute_update(
            sql_dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(dataset_prefixes))
        )

        data_source_impl.execute_update(
            sql_dialect.build_create_table_sql(
                CREATE_TABLE_IF_NOT_EXISTS(
                    fully_qualified_table_name=target_table,
                    columns=[
                        CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))),
                        CREATE_TABLE_COLUMN(
                            name="name",
                            type=SqlDataType(name=type_name(SodaDataTypeName.VARCHAR), character_maximum_length=255),
                        ),
                        CREATE_TABLE_COLUMN(name="n", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))),
                    ],
                    primary_key_column_names=["id"],
                    update_heavy=True,
                )
            )
        )
        create_index_sql: str | None = sql_dialect.build_create_index_sql(
            CREATE_INDEX_IF_NOT_EXISTS(index_name=INDEX, fully_qualified_table_name=target_table, columns=["name"])
        )
        if create_index_sql is not None:
            data_source_impl.execute_update(create_index_sql)
            data_source_impl.execute_update(create_index_sql)

        if data_source_impl.type_name == "postgres":
            catalog_rows = data_source_impl.execute_query(
                sql_dialect.build_select_sql(
                    [
                        SELECT([COLUMN("reloptions", "c")]),
                        FROM("pg_class", table_prefix=["pg_catalog"], alias="c"),
                        JOIN(
                            "pg_namespace",
                            table_prefix=["pg_catalog"],
                            alias="ns",
                            on_condition=EQ(COLUMN("relnamespace", "c"), COLUMN("oid", "ns")),
                        ),
                        WHERE(
                            AND(
                                [
                                    EQ(COLUMN("nspname", "ns"), LITERAL(dataset_prefixes[1])),
                                    EQ(COLUMN("relname", "c"), LITERAL(TARGET)),
                                ]
                            )
                        ),
                    ]
                )
            ).rows
            assert catalog_rows == [(["fillfactor=70"],)]
            index_rows = data_source_impl.execute_query(
                sql_dialect.build_select_sql(
                    [
                        SELECT([COLUMN("indexname")]),
                        FROM("pg_indexes", table_prefix=["pg_catalog"]),
                        WHERE(
                            AND(
                                [
                                    EQ(COLUMN("schemaname"), LITERAL(dataset_prefixes[1])),
                                    EQ(COLUMN("indexname"), LITERAL(INDEX)),
                                ]
                            )
                        ),
                    ]
                )
            ).rows
            assert index_rows == [(INDEX,)]

        # A key that is already there is skipped without update assignments...
        assert upsert([(1, "a", 10), (2, "b", 20)], update_assignments=None) == 2
        assert upsert([(2, "skipped", 0)], update_assignments=None) == 0
        assert target_rows() == [(1, "a", 10), (2, "b", 20)]

        # ...and updated from the incoming row with them. The row count is the inserted plus the updated rows.
        assert (
            upsert([(1, "a2", 11), (3, "c", 30)], update_assignments=[ASSIGNMENT("name", SOURCE_COLUMN("name"))]) == 2
        )
        assert target_rows() == [(1, "a2", 10), (2, "b", 20), (3, "c", 30)]

        data_source_impl.execute_update(
            sql_dialect.build_create_table_sql(
                CREATE_TABLE_IF_NOT_EXISTS(
                    fully_qualified_table_name=source_table,
                    columns=[
                        CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))),
                        CREATE_TABLE_COLUMN(name="n", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))),
                    ],
                )
            )
        )
        data_source_impl.execute_update(
            sql_dialect.build_insert_into_sql(
                INSERT_INTO(
                    fully_qualified_table_name=source_table,
                    columns=[COLUMN("id"), COLUMN("n")],
                    values=[VALUES_ROW([LITERAL(1), LITERAL(100)]), VALUES_ROW([LITERAL(4), LITERAL(400)])],
                )
            )
        )

        data_source_impl.execute_update(
            sql_dialect.build_update_sql(
                UPDATE(
                    fully_qualified_table_name=target_table,
                    alias="tgt",
                    assignments=[ASSIGNMENT("n", COLUMN("n", "src"))],
                    from_elements=[FROM(SOURCE, table_prefix=dataset_prefixes, alias="src")],
                    where=WHERE(EQ(COLUMN("id", "tgt"), COLUMN("id", "src"))),
                )
            )
        )
        assert target_rows() == [(1, "a2", 100), (2, "b", 20), (3, "c", 30)]

        upserted_rows: int = data_source_impl.execute_update(
            sql_dialect.build_upsert_via_select_sql(
                UPSERT_VIA_SELECT(
                    fully_qualified_table_name=target_table,
                    select_elements=[
                        SELECT([COLUMN("id"), COLUMN("n")]),
                        FROM(SOURCE, table_prefix=dataset_prefixes),
                    ],
                    columns=[COLUMN("id"), COLUMN("n")],
                    key_columns=[COLUMN("id")],
                    update_assignments=[ASSIGNMENT("n", SOURCE_COLUMN("n"))],
                )
            )
        )
        assert upserted_rows == 2
        assert target_rows() == [(1, "a2", 100), (2, "b", 20), (3, "c", 30), (4, None, 400)]

        data_source_impl.execute_update(
            sql_dialect.build_delete_sql(
                DELETE(
                    fully_qualified_table_name=target_table,
                    alias="tgt",
                    using_elements=[FROM(SOURCE, table_prefix=dataset_prefixes, alias="src")],
                    where=WHERE(
                        AND([EQ(COLUMN("id", "tgt"), COLUMN("id", "src")), GT(COLUMN("n", "src"), LITERAL(200))])
                    ),
                )
            )
        )
        assert target_rows() == [(1, "a2", 100), (2, "b", 20), (3, "c", 30)]

        data_source_impl.execute_update(
            sql_dialect.build_delete_sql(
                DELETE(fully_qualified_table_name=target_table, where=WHERE(EQ(COLUMN("name"), LITERAL("b"))))
            )
        )
        assert target_rows() == [(1, "a2", 100), (3, "c", 30)]

        analyze_table_sql: str | None = sql_dialect.build_analyze_table_sql(ANALYZE_TABLE(target_table))
        if analyze_table_sql is not None:
            data_source_impl.execute_update(analyze_table_sql)

        # The escape makes `_` literal, so only the name that really contains one matches.
        upsert([(5, "x_y", 50), (6, "xzy", 60)], update_assignments=None)
        like_rows = data_source_impl.execute_query(
            sql_dialect.build_select_sql(
                [
                    SELECT([COLUMN("id")]),
                    FROM(TARGET, table_prefix=dataset_prefixes),
                    WHERE(LIKE(COLUMN("name"), LITERAL("x!_y"), escape="!")),
                ]
            )
        ).rows
        assert like_rows == [(5,)]
    finally:
        drop_tables()


# Which expressions render depends on server facts read at connect (SQL Server), which a replay never reads.
@pytest.mark.no_snapshot
def test_data_plane_expressions_end_to_end(data_source_test_helper: DataSourceTestHelper):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    if not sql_dialect.SUPPORTS_DATA_PLANE_STATEMENTS:
        pytest.skip(f"{type(sql_dialect).__name__} does not render the data-plane expressions")

    dataset_prefixes: list[str] = data_source_test_helper.dataset_prefix
    fixes_table: str = sql_dialect.qualify_dataset_name(dataset_prefixes, FIXES)
    json_type_name: str = JSON_TYPE_NAME_BY_DATA_SOURCE_TYPE[data_source_impl.type_name]

    def type_name(soda_type: SodaDataTypeName) -> str:
        return sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()[soda_type]

    def select_row(*expressions) -> tuple:
        return data_source_impl.execute_query(sql_dialect.build_select_sql([SELECT(list(expressions))])).rows[0]

    def renders(expression) -> bool:
        try:
            sql_dialect.build_expression_sql(expression)
        except UnsupportedSqlStatementError:
            return False
        return True

    def assert_selects(expressions_and_values: list[tuple]) -> None:
        """Selects, in one row, every expression the dialect renders, and compares each with its value."""
        rendered: list[tuple] = [case for case in expressions_and_values if renders(case[0])]
        assert select_row(*(expression for expression, _ in rendered)) == tuple(value for _, value in rendered)

    def json_value(value):
        # Parsed where the data source has a JSON type, text where the column is a string type.
        return json.loads(value) if isinstance(value, str) else value

    def drop_table() -> None:
        data_source_impl.execute_update(
            sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=fixes_table))
        )

    def fixes_rows() -> list[tuple]:
        return data_source_impl.execute_query(
            sql_dialect.build_select_sql(
                [
                    SELECT(
                        [COLUMN("id"), COLUMN("fix_count"), COLUMN("attrs"), COLUMN("created_at"), COLUMN("fixed_at")]
                    ),
                    FROM(FIXES, table_prefix=dataset_prefixes),
                    ORDER_BY_ASC(COLUMN("id")),
                ]
            )
        ).rows

    merges_json: bool = renders(JSON_MERGE(COLUMN("attrs", "cur"), SOURCE_COLUMN("attrs")))

    def record_fix(fix_id: int, attrs_json: str) -> None:
        # A repeated fix counts up and merges its attributes over the ones already recorded, where the
        # dialect merges JSON; elsewhere it keeps them.
        data_source_impl.execute_update(
            sql_dialect.build_upsert_sql(
                UPSERT(
                    fully_qualified_table_name=fixes_table,
                    columns=[COLUMN("id"), COLUMN("fix_count"), COLUMN("attrs"), COLUMN("fixed_at")],
                    values=[
                        VALUES_ROW(
                            [
                                LITERAL(fix_id),
                                LITERAL(1),
                                CAST(LITERAL(attrs_json), json_type_name),
                                CURRENT_TIMESTAMP(),
                            ]
                        )
                    ],
                    key_columns=[COLUMN("id")],
                    update_assignments=[
                        ASSIGNMENT("fix_count", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1))),
                        *(
                            [ASSIGNMENT("attrs", JSON_MERGE(COLUMN("attrs", "cur"), SOURCE_COLUMN("attrs")))]
                            if merges_json
                            else []
                        ),
                        ASSIGNMENT("fixed_at", CURRENT_TIMESTAMP()),
                    ],
                    alias="cur",
                )
            )
        )

    timestamp_type: str = type_name(SodaDataTypeName.TIMESTAMP)
    microseconds = CAST(LITERAL("2024-03-05 07:08:09.123456"), timestamp_type)
    whole_second = CAST(LITERAL("2024-03-05 07:08:09"), timestamp_type)
    hour_later = CAST(LITERAL("2024-03-05 08:08:39"), timestamp_type)
    noon_utc = CAST(LITERAL("2024-03-05 12:00:00+00:00"), type_name(SodaDataTypeName.TIMESTAMP_TZ))
    a_date = CAST(LITERAL("2024-03-05"), type_name(SodaDataTypeName.DATE))

    assert_selects(
        [
            (TIMESTAMP_ISO_TEXT(microseconds), "2024-03-05T07:08:09.123456"),
            (TIMESTAMP_ISO_TEXT(microseconds, fractional_seconds=False), "2024-03-05T07:08:09"),
            (TIMESTAMP_ISO_TEXT(whole_second), "2024-03-05T07:08:09.000000"),
            (TIMESTAMP_ISO_TEXT(a_date, fractional_seconds=False), "2024-03-05T00:00:00"),
            (DATE_ISO_TEXT(a_date), "2024-03-05"),
            (TO_TIMEZONE(noon_utc, "UTC"), datetime(2024, 3, 5, 12, 0)),
            (TIMESTAMP_ISO_TEXT(TO_TIMEZONE(noon_utc, "UTC"), fractional_seconds=False), "2024-03-05T12:00:00"),
            (TIMESTAMP_ISO_TEXT(TO_TIMEZONE(noon_utc, "UTC"), utc_suffix=True), "2024-03-05T12:00:00.000000+00:00"),
            (
                TIMESTAMP_ISO_TEXT(TO_TIMEZONE(noon_utc, "UTC"), fractional_seconds=False, utc_suffix=True),
                "2024-03-05T12:00:00+00:00",
            ),
            (TIMESTAMP_ISO_TEXT(CAST(LITERAL(None), timestamp_type), utc_suffix=True), None),
            (DATE_TRUNC("hour", microseconds), datetime(2024, 3, 5, 7, 0)),
            (DATE_TRUNC("month", microseconds), datetime(2024, 3, 1)),
        ]
    )

    assert_selects(
        [
            (REGEXP_REPLACE(LITERAL("a1b22c333"), "[0-9]+", "#"), "a#b#c#"),
            (REGEXP_REPLACE(LITERAL("-42"), "^(-?[0-9]+)$", "\\1.0"), "-42.0"),
            (REGEXP_REPLACE(LITERAL("it's"), "'", "''"), "it''s"),
            (NULLIF(LITERAL(0), LITERAL(0)), None),
            (NULLIF(LITERAL(5), LITERAL(0)), 5),
            (ABS(LITERAL(-7)), 7),
            (ARITHMETIC("*", LITERAL(6), LITERAL(7)), 42),
            (ARITHMETIC("-", ARITHMETIC("+", LITERAL(1), LITERAL(2)), LITERAL(10)), -7),
        ]
    )

    json_merge = JSON_MERGE(
        CAST(LITERAL('{"a": 1, "b": 1}'), json_type_name), CAST(LITERAL('{"b": 2, "c": 3}'), json_type_name)
    )
    if renders(json_merge):
        assert json_value(select_row(json_merge)[0]) == {"a": 1, "b": 2, "c": 3}

    if data_source_impl.type_name == "postgres":
        # CAST takes a dialect type name as well as a SodaDataTypeName. ARITHMETIC "/" divides the way
        # the data source does: integers truncate on Postgres.
        assert select_row(
            CAST(LITERAL("1.5"), "double precision"),
            CAST(ARITHMETIC("/", LITERAL(7), LITERAL(2)), "numeric"),
            CAST(ARITHMETIC("/", CAST(LITERAL(7), "numeric"), LITERAL(2)), "text"),
        ) == (1.5, 3, "3.5000000000000000")

    # A zone other than UTC is in the data source's own vocabulary (IANA on Postgres), and only a data
    # source that types a timestamp difference as an interval has EPOCH_SECONDS.
    brussels = TO_TIMEZONE(noon_utc, "Europe/Brussels")
    if renders(brussels):
        assert select_row(brussels) == (datetime(2024, 3, 5, 13, 0),)
    epoch_seconds = [
        EPOCH_SECONDS(ARITHMETIC("-", hour_later, whole_second)),
        EPOCH_SECONDS(ARITHMETIC("-", microseconds, whole_second)),
    ]
    if renders(epoch_seconds[0]):
        assert [float(seconds) for seconds in select_row(*epoch_seconds)] == [3630.0, pytest.approx(0.123456)]

    drop_table()
    try:
        data_source_impl.execute_update(
            sql_dialect.build_create_table_sql(
                CREATE_TABLE_IF_NOT_EXISTS(
                    fully_qualified_table_name=fixes_table,
                    columns=[
                        CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))),
                        CREATE_TABLE_COLUMN(
                            name="fix_count", type=SqlDataType(name=type_name(SodaDataTypeName.INTEGER))
                        ),
                        CREATE_TABLE_COLUMN(name="attrs", type=SqlDataType(name=json_type_name)),
                        CREATE_TABLE_COLUMN(
                            name="created_at",
                            type=SqlDataType(name=type_name(SodaDataTypeName.TIMESTAMP_TZ)),
                            default=CURRENT_TIMESTAMP(),
                        ),
                        CREATE_TABLE_COLUMN(
                            name="fixed_at", type=SqlDataType(name=type_name(SodaDataTypeName.TIMESTAMP_TZ))
                        ),
                    ],
                    primary_key_column_names=["id"],
                    update_heavy=True,
                )
            )
        )

        record_fix(1, '{"a": 1, "b": 1}')
        record_fix(2, '{"z": 0}')
        record_fix(1, '{"b": 2, "c": 3}')

        data_source_impl.execute_update(
            sql_dialect.build_upsert_via_select_sql(
                UPSERT_VIA_SELECT(
                    fully_qualified_table_name=fixes_table,
                    select_elements=[SELECT([LITERAL(2), LITERAL(5)])],
                    columns=[COLUMN("id"), COLUMN("fix_count")],
                    key_columns=[COLUMN("id")],
                    update_assignments=[
                        ASSIGNMENT("fix_count", ARITHMETIC("+", COLUMN("fix_count", "cur"), SOURCE_COLUMN("fix_count")))
                    ],
                    alias="cur",
                )
            )
        )

        rows = fixes_rows()
        first_attrs: dict = {"a": 1, "b": 2, "c": 3} if merges_json else {"a": 1, "b": 1}
        assert [(row[0], row[1], json_value(row[2])) for row in rows] == [(1, 2, first_attrs), (2, 6, {"z": 0})]
        # created_at comes from the column default, fixed_at from a CURRENT_TIMESTAMP value.
        for _, _, _, created_at, fixed_at in rows:
            assert isinstance(created_at, datetime) and isinstance(fixed_at, datetime)
            assert fixed_at >= created_at
    finally:
        drop_table()
