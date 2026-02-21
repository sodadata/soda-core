import datetime

import pytz
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.datetime_conversions import interpret_datetime_as_utc
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    ALTER_TABLE_ADD_COLUMN,
    ALTER_TABLE_DROP_COLUMN,
    COLUMN,
    COMBINED_HASH,
    COUNT,
    CREATE_TABLE_AS_SELECT,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CREATE_VIEW,
    CTE,
    DROP_TABLE_IF_EXISTS,
    DROP_VIEW_IF_EXISTS,
    FROM,
    INSERT_INTO,
    LITERAL,
    ORDER_BY_ASC,
    SELECT,
    STAR,
    VALUES_ROW,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect


def test_full_create_insert_drop_ast(data_source_test_helper: DataSourceTestHelper):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    dataset_prefixes = data_source_test_helper.dataset_prefix

    my_table_name = sql_dialect.qualify_dataset_name(dataset_prefixes, "my_test_test_table")
    my_table_name_as_select = sql_dialect.qualify_dataset_name(dataset_prefixes, "my_test_test_table_as_select")
    my_view_name = sql_dialect.qualify_dataset_name(dataset_prefixes, "my_test_test_view")

    # Drop the view if it exists (do this first, otherwise the table drop will fail because we should "cascade" the drop)
    drop_view_sql = sql_dialect.build_drop_view_sql(DROP_VIEW_IF_EXISTS(fully_qualified_view_name=my_view_name))
    data_source_impl.execute_update(drop_view_sql)

    # Drop table if exists
    drop_table_sql = sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name))
    data_source_impl.execute_update(drop_table_sql)
    drop_table_sql = sql_dialect.build_drop_table_sql(
        DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name_as_select)
    )
    data_source_impl.execute_update(drop_table_sql)

    def col_type(name: str) -> str:
        return sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()[name]

    try:
        create_table_columns = [
            CREATE_TABLE_COLUMN(
                name="id",
                type=SqlDataType(name=col_type(SodaDataTypeName.INTEGER)),
                nullable=False,
            ),
            CREATE_TABLE_COLUMN(
                name="name",
                type=SqlDataType(
                    name=col_type(SodaDataTypeName.VARCHAR),
                    character_maximum_length=255,
                ),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(
                name="small_text",
                type=SqlDataType(name=col_type(SodaDataTypeName.VARCHAR), character_maximum_length=3),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(
                name="my_date",
                type=SqlDataType(name=col_type(SodaDataTypeName.DATE)),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(
                name="my_timestamp",
                type=SqlDataType(name=col_type(SodaDataTypeName.TIMESTAMP)),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(
                name="my_timestamp_tz",
                type=SqlDataType(name=col_type(SodaDataTypeName.TIMESTAMP_TZ)),
                nullable=True,
            ),
        ]

        standard_columns = [column.convert_to_standard_column() for column in create_table_columns]

        # First create the table
        create_table_sql = sql_dialect.build_create_table_sql(
            CREATE_TABLE_IF_NOT_EXISTS(
                fully_qualified_table_name=my_table_name,
                columns=create_table_columns,
            )
        )
        data_source_impl.execute_update(create_table_sql)

        # Then add another column, just because we can
        extra_column: CREATE_TABLE_COLUMN = CREATE_TABLE_COLUMN(
            name="some_number",
            type=SqlDataType(name=SodaDataTypeName.INTEGER),
            nullable=True,
        )
        alter_table_add_column_sql = sql_dialect.build_alter_table_sql(
            ALTER_TABLE_ADD_COLUMN(fully_qualified_table_name=my_table_name, column=extra_column)
        )
        data_source_impl.execute_update(alter_table_add_column_sql)
        # Add the column to the standard columns list
        standard_columns.append(extra_column.convert_to_standard_column())

        # Check the metadata, we want the columns to be in the correct order
        metadata_columns_query_sql = data_source_impl.build_columns_metadata_query_str(
            dataset_prefixes=dataset_prefixes,
            dataset_name="my_test_test_table",
        )
        metadata_result: QueryResult = data_source_impl.execute_query(metadata_columns_query_sql)
        assert metadata_result.rows[0][0] == "id"
        assert metadata_result.rows[1][0] == "name"
        assert metadata_result.rows[2][0] == "small_text"
        assert metadata_result.rows[3][0] == "my_date"
        assert metadata_result.rows[4][0] == "my_timestamp"
        assert metadata_result.rows[5][0] == "my_timestamp_tz"
        assert metadata_result.rows[6][0] == "some_number"

        # Then insert into the table
        tz = pytz.timezone("America/Los_Angeles")  # to test a non-UTC timezone
        insert_into_sql = sql_dialect.build_insert_into_sql(
            INSERT_INTO(
                fully_qualified_table_name=my_table_name,
                values=[
                    VALUES_ROW(
                        [
                            LITERAL(1),
                            LITERAL("John"),
                            LITERAL("a"),
                            LITERAL(datetime.date(2021, 1, 1)),
                            LITERAL(datetime.datetime(2021, 1, 1, 10, 0, 0)),
                            LITERAL(datetime.datetime(2021, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)),
                            LITERAL(100),
                        ]
                    ),
                    VALUES_ROW(
                        [
                            LITERAL(2),
                            LITERAL("Jane"),
                            LITERAL("b"),
                            LITERAL(datetime.date(2021, 1, 2)),
                            LITERAL(datetime.datetime(2021, 1, 2, 10, 0, 0)),
                            LITERAL(tz.localize(datetime.datetime(2021, 1, 2, 10, 0, 0))),
                            LITERAL(25),
                        ]
                    ),
                ],
                columns=standard_columns,
            )
        )
        data_source_impl.execute_update(insert_into_sql)

        insert_with_columns_sql = sql_dialect.build_insert_into_sql(
            INSERT_INTO(
                fully_qualified_table_name=my_table_name,
                columns=[COLUMN("id"), COLUMN("name")],
                values=[
                    VALUES_ROW([LITERAL(3), LITERAL("Bob")]),
                ],
            )
        )
        data_source_impl.execute_update(insert_with_columns_sql)

        # Then select from the table
        select_sql = sql_dialect.build_select_sql(
            [
                SELECT(
                    [
                        COLUMN("id"),
                        COLUMN("name"),
                        COLUMN("small_text"),
                        COLUMN("my_date"),
                        COLUMN("my_timestamp"),
                        COLUMN("my_timestamp_tz"),
                        COLUMN("some_number"),
                    ]
                ),
                # This has to be changed in the select sql. We should expect the fully qualified table name, like with other ASTs
                # This is a fix for now, as Athena does not support quoted table names, so we don't need to drop the "quotes".
                FROM(sql_dialect.get_from_name_from_qualified_name(my_table_name)),
                ORDER_BY_ASC(COLUMN("id")),
            ]
        )
        result: QueryResult = data_source_impl.execute_query(select_sql)

        # Yes, this is a bit ugly, but it's the only way to test the results consistently across datasources
        assert result.rows[0][1] == "John"
        assert result.rows[1][1] == "Jane"
        assert result.rows[2][1] == "Bob"

        assert result.rows[0][2] == "a"
        assert result.rows[1][2] == "b"
        assert result.rows[2][2] is None

        # some db engines (e.g. oracle) store dates with a time of 00:00:00
        assert result.rows[0][3] in [
            datetime.date(2021, 1, 1),
            datetime.datetime(2021, 1, 1, 0, 0, 0),
        ]
        assert result.rows[1][3] in [
            datetime.date(2021, 1, 2),
            datetime.datetime(2021, 1, 2, 0, 0, 0),
        ]
        assert result.rows[2][3] is None

        assert result.rows[0][4] in [
            datetime.datetime(2021, 1, 1, 10, 0, 0),
            datetime.datetime(2021, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc),
        ]
        assert result.rows[1][4] in [
            datetime.datetime(2021, 1, 2, 10, 0, 0),
            datetime.datetime(2021, 1, 2, 10, 0, 0, tzinfo=datetime.timezone.utc),
        ]
        assert result.rows[2][4] is None

        # Check that the timezone is correctly set, otherwise assume utc.
        assert interpret_datetime_as_utc(result.rows[0][5]) == datetime.datetime(
            2021, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc
        )
        assert interpret_datetime_as_utc(result.rows[1][5]) == tz.localize(datetime.datetime(2021, 1, 2, 10, 0, 0))
        assert result.rows[2][5] is None

        assert result.rows[0][6] == 100
        assert result.rows[1][6] == 25
        assert result.rows[2][6] is None

        # Then drop the extra column, if supported
        if sql_dialect.drop_column_supported():
            alter_table_drop_column_sql = sql_dialect.build_alter_table_sql(
                ALTER_TABLE_DROP_COLUMN(fully_qualified_table_name=my_table_name, column_name="some_number")
            )
            data_source_impl.execute_update(alter_table_drop_column_sql)

        # Then create a table as a select
        create_table_as_select_sql = sql_dialect.build_create_table_as_select_sql(
            CREATE_TABLE_AS_SELECT(
                fully_qualified_table_name=my_table_name_as_select,
                select_elements=[
                    # Use a CTE just to cover more edge cases
                    WITH(
                        cte_list=[
                            CTE(
                                alias="my_cte",
                                cte_query=[
                                    SELECT(STAR()),
                                    FROM(sql_dialect.get_from_name_from_qualified_name(my_table_name)),
                                ],
                            ),
                        ]
                    ),
                    SELECT(STAR()),
                    FROM("my_cte"),
                ],
            )
        )
        data_source_impl.execute_update(create_table_as_select_sql)
        # Then query the row count from that new table
        select_sql = sql_dialect.build_select_sql(
            [
                SELECT([COUNT(STAR())]),
                FROM(sql_dialect.get_from_name_from_qualified_name(my_table_name_as_select)),
            ]
        )
        result: QueryResult = data_source_impl.execute_query(select_sql)
        assert result.rows[0][0] == 3

        if data_source_impl.can_create_view:
            # Then create a view
            create_view_sql = sql_dialect.build_create_view_sql(
                CREATE_VIEW(
                    fully_qualified_view_name=my_view_name,
                    select_elements=[
                        SELECT(STAR()),
                        FROM(sql_dialect.get_from_name_from_qualified_name(my_table_name)),
                    ],
                )
            )
            if data_source_impl.try_create_view(create_view_sql):
                # Then query the view
                select_view_sql = sql_dialect.build_select_sql(
                    [
                        SELECT(STAR()),
                        FROM(sql_dialect.get_from_name_from_qualified_name(my_view_name)),
                        ORDER_BY_ASC(
                            COLUMN("id")
                        ),  # To make it deterministic (some datasources don't guarantee order of rows)
                    ]
                )
                result: QueryResult = data_source_impl.execute_query(select_view_sql)
                assert result.rows[0][0] == 1
                assert result.rows[1][0] == 2
                assert result.rows[2][0] == 3
    finally:
        # Drop the view first, otherwise the table drop will fail because we should "cascade" the drop
        if data_source_impl.can_create_view:
            drop_view_sql = sql_dialect.build_drop_view_sql(DROP_VIEW_IF_EXISTS(fully_qualified_view_name=my_view_name))
            data_source_impl.execute_update(drop_view_sql)
        # Then drop the table to clean up
        drop_table_sql = sql_dialect.build_drop_table_sql(
            DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name)
        )
        data_source_impl.execute_update(drop_table_sql)
        # Table we created with the "as select"
        drop_table_sql = sql_dialect.build_drop_table_sql(
            DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name_as_select)
        )
        data_source_impl.execute_update(drop_table_sql)


def test_large_insert_test_table(data_source_test_helper: DataSourceTestHelper):
    NUMBER_OF_ROWS = 2025
    large_test_table_specification = (
        TestTableSpecification.builder()
        .table_purpose("testing_large_inserts")
        .column_varchar("id", 100)
        .column_varchar("id2", 100)
        .column_integer("my_value")
        .rows(rows=[(f"my_id_{i}", f"my_id2_{i}", 10) for i in range(NUMBER_OF_ROWS)])
        .build()
    )

    test_table = data_source_test_helper.ensure_test_table(large_test_table_specification)
    my_table_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)

    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    select_sql = sql_dialect.build_select_sql(
        [
            SELECT([COLUMN("id"), COLUMN("id2"), COLUMN("my_value")]),
            FROM(my_table_name[1:-1] if sql_dialect.is_quoted(my_table_name) else my_table_name),
        ]
    )
    result: QueryResult = data_source_test_helper.data_source_impl.execute_query(select_sql)
    assert len(result.rows) == NUMBER_OF_ROWS


def test_datetime_microsecond_precision_insert(
    data_source_test_helper: DataSourceTestHelper,
):
    NUMBER_OF_ROWS = 20
    microsecond_test_table_specification = (
        TestTableSpecification.builder()
        .table_purpose("testing_microsecond_inserts")
        .column_varchar("id", 100)
        .column_varchar("id2", 100)
        .column_integer("my_value")
        .column_timestamp("my_timestamp", datetime_precision=6)
        .rows(
            rows=[
                (
                    f"my_id_{i}",
                    f"my_id2_{i}",
                    10,
                    datetime.datetime(2021, 1, 1, i, 0, 0, microsecond=123),
                )
                for i in range(NUMBER_OF_ROWS)
            ]
        )
        .build()
    )

    test_table = data_source_test_helper.ensure_test_table(microsecond_test_table_specification, force_recreate=True)
    my_table_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)

    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    select_sql = sql_dialect.build_select_sql(
        [
            SELECT(
                [
                    COLUMN("id"),
                    COLUMN("id2"),
                    COLUMN("my_value"),
                    COLUMN("my_timestamp"),
                ]
            ),
            FROM(my_table_name[1:-1] if sql_dialect.is_quoted(my_table_name) else my_table_name),
            ORDER_BY_ASC(COLUMN("my_timestamp")),
        ]
    )
    result: QueryResult = data_source_test_helper.data_source_impl.execute_query(select_sql)
    assert len(result.rows) == NUMBER_OF_ROWS
    for i in range(NUMBER_OF_ROWS):
        if data_source_test_helper.data_source_impl.sql_dialect.supports_datetime_microseconds():
            # We're not here to check the timezone returned, only that the microsecond is correct
            returned_datetime = result.rows[i][3].replace(tzinfo=None)
            assert returned_datetime == datetime.datetime(2021, 1, 1, i, 0, 0, 123)
        else:
            # We're not here to check the timezone returned, only that the microsecond is correct.
            returned_datetime = result.rows[i][3].replace(tzinfo=None)
            assert returned_datetime == datetime.datetime(2021, 1, 1, i, 0, 0)


def test_hashing_output_text(data_source_test_helper: DataSourceTestHelper):
    NUMBER_OF_ROWS = 20
    hashing_test_table_specification = (
        TestTableSpecification.builder()
        .table_purpose("testing_hashing_output_text")
        .column_varchar("id", 100)
        .column_varchar("id2", 100)
        .column_integer("my_value")
        .rows(rows=[(f"my_id_{i}", f"my_id2_{i}", 10) for i in range(NUMBER_OF_ROWS)])
        .build()
    )

    test_table = data_source_test_helper.ensure_test_table(hashing_test_table_specification, force_recreate=True)
    my_table_name = data_source_test_helper.get_qualified_name_from_test_table(test_table)

    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    select_sql = sql_dialect.build_select_sql(
        [
            SELECT([COMBINED_HASH([COLUMN("id"), COLUMN("id2"), COLUMN("my_value")])]),
            FROM(my_table_name[1:-1] if sql_dialect.is_quoted(my_table_name) else my_table_name),
        ]
    )
    result: QueryResult = data_source_test_helper.data_source_impl.execute_query(select_sql)
    assert len(result.rows) == NUMBER_OF_ROWS
    for i in range(NUMBER_OF_ROWS):
        assert result.rows[i][0] is not None
        assert isinstance(result.rows[i][0], str)
