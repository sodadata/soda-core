from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.metadata_types import (
    ColumnMetadata,
    SodaDataTypeName,
    SqlDataType,
)
from soda_core.common.sql_dialect import SqlDialect

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metadata")
    .column_varchar(name="varchar_default")
    .column_varchar(name="varchar_w_length", character_maximum_length=255)
    .column_integer(name="integer_default")
    .column_numeric(name="numeric_default")
    .column_numeric(name="numeric_w_precision", numeric_precision=10)
    .column_numeric(name="numeric_w_precision_and_scale", numeric_precision=10, numeric_scale=2)
    .column_timestamp(name="ts_default")
    .column_timestamp(name="ts_w_precision", datetime_precision=2)
    .column_timestamp_tz(name="ts_tz_default")
    .column_timestamp_tz(name="ts_tz_w_precision", datetime_precision=4)
    .build()
)


def test_table_metadata(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    sql_dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect

    actual_columns: list[ColumnMetadata] = data_source_test_helper.data_source_impl.get_columns_metadata(
        dataset_prefixes=test_table.dataset_prefix,
        dataset_name=test_table.unique_name
    )

    sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR)

    actual_txt_default: ColumnMetadata = actual_columns[0]
    assert actual_txt_default.column_name == "varchar_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR)),
        actual=actual_txt_default.sql_data_type,
    )

    actual_txt_w_length: ColumnMetadata = actual_columns[1]
    assert actual_txt_w_length.column_name == "varchar_w_length"
    length = 255 if sql_dialect.supports_data_type_character_maximun_length() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR),
            character_maximum_length=length,
        ),
        actual=actual_txt_w_length.sql_data_type,
    )

    actual_integer_default: ColumnMetadata = actual_columns[2]
    assert actual_integer_default.column_name == "integer_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.INTEGER),
        ),
        actual=actual_integer_default.sql_data_type,
    )

    actual_numeric_default: ColumnMetadata = actual_columns[3]
    assert actual_numeric_default.column_name == "numeric_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
        ),
        actual=actual_numeric_default.sql_data_type,
    )

    actual_numeric_w_precision: ColumnMetadata = actual_columns[4]
    assert actual_numeric_w_precision.column_name == "numeric_w_precision"
    precision = 10 if sql_dialect.supports_data_type_numeric_precision() else None
    scale = 0 if sql_dialect.supports_data_type_numeric_scale() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
            numeric_precision=precision,
            numeric_scale=scale,
        ),
        actual=actual_numeric_w_precision.sql_data_type,
    )

    actual_numeric_w_precision_and_scale: ColumnMetadata = actual_columns[5]
    assert actual_numeric_w_precision_and_scale.column_name == "numeric_w_precision_and_scale"
    precision = 10 if sql_dialect.supports_data_type_numeric_precision() else None
    scale = 2 if sql_dialect.supports_data_type_numeric_scale() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.NUMERIC),
            numeric_precision=precision,
            numeric_scale=scale,
        ),
        actual=actual_numeric_w_precision_and_scale.sql_data_type,
    )

    actual_ts_default: ColumnMetadata = actual_columns[6]
    assert actual_ts_default.column_name == "ts_default"
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.TIMESTAMP),
        ),
        actual=actual_ts_default.sql_data_type,
    )

    actual_ts_w_precision: ColumnMetadata = actual_columns[7]
    assert actual_ts_w_precision.column_name == "ts_w_precision"
    precision = 2 if sql_dialect.supports_data_type_datetime_precision() else None
    assert sql_dialect.is_same_data_type_for_schema_check(
        expected=SqlDataType(
            name=sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.TIMESTAMP),
            datetime_precision=precision,
        ),
        actual=actual_ts_w_precision.sql_data_type,
    )
