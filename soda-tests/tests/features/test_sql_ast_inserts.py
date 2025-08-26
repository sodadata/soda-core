import datetime

import pytz
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import (
    COLUMN,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    DROP_TABLE_IF_EXISTS,
    FROM,
    INSERT_INTO,
    LITERAL,
    ORDER_BY_ASC,
    SELECT,
    VALUES_ROW,
)
from soda_core.common.sql_dialect import SqlDialect


def test_full_create_insert_drop_ast(data_source_test_helper: DataSourceTestHelper):
    data_source_impl: DataSourceImpl = data_source_test_helper.data_source_impl
    sql_dialect: SqlDialect = data_source_impl.sql_dialect
    dataset_prefix = data_source_test_helper.dataset_prefix

    my_table_name = sql_dialect.qualify_dataset_name(dataset_prefix, "my_test_test_table")

    # Drop table if exists
    drop_table_sql = sql_dialect.build_drop_table_sql(DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name))
    data_source_impl.execute_update(drop_table_sql)

    def col_type(name: str) -> str:
        return sql_dialect.get_sql_data_type_name_by_soda_data_type_names()[name]

    try:
        create_table_columns = [
            CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name=col_type(SodaDataTypeName.INTEGER)), nullable=False),
            CREATE_TABLE_COLUMN(
                name="name",
                type=SqlDataType(name=col_type(SodaDataTypeName.VARCHAR), character_maximum_length=255),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(
                name="small_text",
                type=SqlDataType(name=col_type(SodaDataTypeName.VARCHAR), character_maximum_length=3),
                nullable=True,
            ),
            CREATE_TABLE_COLUMN(name="my_date", type=SqlDataType(name=col_type(SodaDataTypeName.DATE)), nullable=True),
            CREATE_TABLE_COLUMN(name="my_timestamp", type=SqlDataType(name=col_type(SodaDataTypeName.TIMESTAMP)), nullable=True),
            CREATE_TABLE_COLUMN(
                name="my_timestamp_tz", type=SqlDataType(name=col_type(SodaDataTypeName.TIMESTAMP_TZ)), nullable=True
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

        # Check the metadata, we want the columns to be in the correct order
        metadata_columns_query = data_source_impl.create_metadata_columns_query()
        metadata_columns_query_sql = metadata_columns_query.build_sql(
            dataset_prefix=dataset_prefix,
            dataset_name="my_test_test_table",
        )
        metadata_result: QueryResult = data_source_impl.execute_query(metadata_columns_query_sql)
        assert metadata_result.rows[0][0] == "id"
        assert metadata_result.rows[1][0] == "name"
        assert metadata_result.rows[2][0] == "small_text"
        assert metadata_result.rows[3][0] == "my_date"

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
                    ]
                ),
                FROM(my_table_name[1:-1]),
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
        assert result.rows[0][3] in [datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 0, 0, 0)]
        assert result.rows[1][3] in [datetime.date(2021, 1, 2), datetime.datetime(2021, 1, 2, 0, 0, 0)]
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

        assert result.rows[0][5] == datetime.datetime(2021, 1, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)
        assert result.rows[1][5] == tz.localize(datetime.datetime(2021, 1, 2, 10, 0, 0))
        assert result.rows[2][5] is None

    finally:
        # Then drop the table to clean up
        # We explicitly do not use the "if exists" variant, because the table should exist at this point.
        drop_table_sql = sql_dialect.build_drop_table_sql(
            DROP_TABLE_IF_EXISTS(fully_qualified_table_name=my_table_name)
        )
        data_source_impl.execute_update(drop_table_sql)
