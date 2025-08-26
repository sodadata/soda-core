from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType, ColumnMetadata
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery

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

    metadata_columns_query: MetadataColumnsQuery = (
        data_source_test_helper.data_source_impl.create_metadata_columns_query()
    )
    get_columns_metadata_query_sql: str = metadata_columns_query.build_sql(
        dataset_prefix=test_table.dataset_prefix, dataset_name=test_table.unique_name
    )
    query_result: QueryResult = data_source_test_helper.data_source_impl.execute_query(get_columns_metadata_query_sql)
    actual_columns: list[ColumnMetadata] = metadata_columns_query.get_result(query_result)

    sql_dialect.get_sql_data_type_name_for_soda_data_type_name(SodaDataTypeName.VARCHAR)

    actual_txt_default: ColumnMetadata = actual_columns[0]
    assert actual_txt_default.column_name == "varchar_default"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_txt_default.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.VARCHAR
        )
    )
    assert actual_txt_default.sql_data_type.character_maximum_length is None
    assert actual_txt_default.sql_data_type.numeric_precision is None
    assert actual_txt_default.sql_data_type.numeric_scale is None
    assert actual_txt_default.sql_data_type.datetime_precision is None

    actual_txt_w_length: ColumnMetadata = actual_columns[1]
    assert actual_txt_w_length.column_name == "varchar_w_length"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_txt_w_length.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.VARCHAR
        )
    )
    assert actual_txt_w_length.sql_data_type.character_maximum_length == 255
    assert actual_txt_w_length.sql_data_type.numeric_precision is None
    assert actual_txt_w_length.sql_data_type.numeric_scale is None
    assert actual_txt_w_length.sql_data_type.datetime_precision is None

    actual_integer_default: ColumnMetadata = actual_columns[2]
    assert actual_integer_default.column_name == "integer_default"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_integer_default.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.INTEGER
        )
    )
    assert actual_integer_default.sql_data_type.character_maximum_length is None
    assert actual_integer_default.sql_data_type.numeric_precision is None
    assert actual_integer_default.sql_data_type.numeric_scale is None
    assert actual_integer_default.sql_data_type.datetime_precision is None

    actual_numeric_default: ColumnMetadata = actual_columns[3]
    assert actual_numeric_default.column_name == "numeric_default"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_numeric_default.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.NUMERIC
        )
    )
    assert actual_numeric_default.sql_data_type.character_maximum_length is None
    assert actual_numeric_default.sql_data_type.numeric_precision is None
    assert actual_numeric_default.sql_data_type.numeric_scale is None
    assert actual_numeric_default.sql_data_type.datetime_precision is None

    actual_numeric_w_precision: ColumnMetadata = actual_columns[4]
    assert actual_numeric_w_precision.column_name == "numeric_w_precision"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_numeric_w_precision.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.NUMERIC
        )
    )
    assert actual_numeric_w_precision.sql_data_type.character_maximum_length is None
    assert actual_numeric_w_precision.sql_data_type.numeric_precision == 10
    assert actual_numeric_w_precision.sql_data_type.numeric_scale == 0
    assert actual_numeric_w_precision.sql_data_type.datetime_precision is None

    actual_numeric_w_precision_and_scale: ColumnMetadata = actual_columns[5]
    assert actual_numeric_w_precision_and_scale.column_name == "numeric_w_precision_and_scale"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_numeric_w_precision_and_scale.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.NUMERIC
        )
    )
    assert actual_numeric_w_precision_and_scale.sql_data_type.character_maximum_length is None
    assert actual_numeric_w_precision_and_scale.sql_data_type.numeric_precision == 10
    assert actual_numeric_w_precision_and_scale.sql_data_type.numeric_scale == 2
    assert actual_numeric_w_precision_and_scale.sql_data_type.datetime_precision is None

    actual_ts_default: ColumnMetadata = actual_columns[6]
    assert actual_ts_default.column_name == "ts_default"
    assert sql_dialect.data_type_names_are_same_or_synonym(
        actual_ts_default.sql_data_type.name,
        sql_dialect.get_sql_data_type_name_for_soda_data_type_name(
            SodaDataTypeName.NUMERIC
        )
    )
    assert actual_ts_default.sql_data_type.character_maximum_length is None
    assert actual_ts_default.sql_data_type.numeric_precision is None
    assert actual_ts_default.sql_data_type.numeric_scale is None
    # TODO fix this
    # assert actual_ts_default.sql_data_type.datetime_precision is None

    # TODO
    # .column_timestamp(name="ts_w_precision", datetime_precision=2)
    # .column_timestamp_tz(name="ts_tz_default")
    # .column_timestamp_tz(name="ts_tz_w_precision", datetime_precision=4)
