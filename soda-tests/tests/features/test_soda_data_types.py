import datetime

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import (
    ColumnMetadata,
    SodaDataTypeName,
    SqlDataType,
)


def test_soda_data_types(data_source_test_helper: DataSourceTestHelper):
    dialect_soda_data_type_map: dict = (
        data_source_test_helper.data_source_impl.sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    )
    unmapped_data_types: list[str] = [
        str(soda_data_type) for soda_data_type in SodaDataTypeName if soda_data_type not in dialect_soda_data_type_map
    ]
    assert unmapped_data_types == []


def test_all_data_types_explicitly(data_source_test_helper: DataSourceTestHelper):
    mappings_from_canonical_to_data_type: dict = (
        data_source_test_helper.data_source_impl.sql_dialect.get_data_source_data_type_name_by_soda_data_type_names()
    )

    # Make sure that each SodaDataType is mapped to a data type in the data source
    for soda_data_type in SodaDataTypeName:
        assert soda_data_type in mappings_from_canonical_to_data_type


def test_mapping_canonical_to_data_type_to_canonical(data_source_test_helper: DataSourceTestHelper):
    test_table_specification = (
        TestTableSpecification.builder()
        .table_purpose("testing_information_schema")
        .column_char("char_default")
        .column_char("char_w_length", 100)
        .column_varchar("varchar_default")
        .column_varchar("varchar_w_length", 100)
        .column_text("text_default")
        .column_smallint("smallint_default")
        .column_integer("integer_default")
        .column_bigint("bigint_default")
        .column_decimal("decimal_default")
        .column_decimal("decimal_w_precision", 10)
        .column_decimal("decimal_w_precision_and_scale", 10, 2)
        .column_numeric("numeric_default")
        .column_numeric("numeric_w_precision", 10)
        .column_numeric("numeric_w_precision_and_scale", 10, 2)
        .column_float("float_default")
        .column_double("double_default")
        .column_timestamp("timestamp_default")
        .column_timestamp("timestamp_w_precision", 2)
        .column_timestamp_tz("timestamp_tz_default")
        .column_timestamp_tz("timestamp_tz_w_precision", 4)
        .column_date("date_default")
        .column_time("time_default")
        .column_boolean("boolean_default")
        .rows(
            rows=[
                (
                    # Chars
                    "a",
                    "abc",
                    # Varchars
                    "a",
                    "abc",
                    # Texts
                    "a",
                    # Integers
                    1,
                    100,
                    100000,
                    # Decimals
                    1.0,
                    100.0,
                    100000.1234567890,
                    # Numerics
                    1.0,
                    100.0,
                    100000.1234567890,
                    # Floats
                    1.1234,
                    # Doubles
                    1.1234567890,
                    # Timestamps
                    datetime.datetime(2021, 1, 1, 1, 1, 1),
                    datetime.datetime(2022, 1, 1, 1, 1, 1, 12),
                    # Timestamp Zones
                    datetime.datetime(2023, 1, 1, 1, 1, 1),
                    datetime.datetime(2024, 1, 1, 1, 1, 1, 12),
                    # Dates
                    datetime.date(2025, 1, 1),
                    # Times
                    datetime.time(1, 2, 3),
                    # Booleans
                    True,
                )
            ]
        )
        .build()
    )

    # Building the test table already tests the mapping from canonical to data type
    # Behind the scenes, we build this table using SodaDataTypeName.CHAR, SodaDataTypeName.SMALLINT, etc.
    # So if the table gets created, then the mapping from canonical to data type is correct
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix, dataset_name=test_table.unique_name
    )

    assert len(actual_columns) == 23

    def convert_metadata_to_soda_data_type(metadata: ColumnMetadata) -> SodaDataTypeName:
        sql_data_type: SqlDataType = metadata.sql_data_type
        data_type_name = sql_data_type.name
        return data_source_test_helper.data_source_impl.sql_dialect.get_soda_data_type_name_by_data_source_data_type_names()[
            data_type_name
        ]

    column_mappings = {
        0: SodaDataTypeName.CHAR,
        1: SodaDataTypeName.CHAR,
        2: SodaDataTypeName.VARCHAR,
        3: SodaDataTypeName.VARCHAR,
        4: SodaDataTypeName.TEXT,
        5: SodaDataTypeName.SMALLINT,
        6: SodaDataTypeName.INTEGER,
        7: SodaDataTypeName.BIGINT,
        8: SodaDataTypeName.DECIMAL,
        9: SodaDataTypeName.DECIMAL,
        10: SodaDataTypeName.DECIMAL,
        11: SodaDataTypeName.NUMERIC,
        12: SodaDataTypeName.NUMERIC,
        13: SodaDataTypeName.NUMERIC,
        14: SodaDataTypeName.FLOAT,
        15: SodaDataTypeName.DOUBLE,
        16: SodaDataTypeName.TIMESTAMP,
        17: SodaDataTypeName.TIMESTAMP,
        18: SodaDataTypeName.TIMESTAMP_TZ,
        19: SodaDataTypeName.TIMESTAMP_TZ,
        20: SodaDataTypeName.DATE,
        21: SodaDataTypeName.TIME,
        22: SodaDataTypeName.BOOLEAN,
    }
    for i, column in enumerate(actual_columns):
        print(
            f"Column {i}: {column.column_name} -> Expected: {column_mappings[i]}, Actual: {convert_metadata_to_soda_data_type(column)}"
        )
        assert data_source_test_helper.data_source_impl.sql_dialect.is_same_soda_data_type(
            column_mappings[i], convert_metadata_to_soda_data_type(column)
        )
