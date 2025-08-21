from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import SqlDataType
from soda_core.common.sql_datatypes import DBDataType
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metadata")
    .column(
        column_name="txt_default",
        data_type=SqlDataType(name=DBDataType.VARCHAR)
    )
    .column(
        column_name="txt_w_length",
        data_type=SqlDataType(name=DBDataType.VARCHAR, character_maximum_length=255)
    )
    .column(
        column_name="dec_default",
        data_type=SqlDataType(name=DBDataType.DECIMAL)
    )
    .column(
        column_name="dec_w_precision",
        data_type=SqlDataType(name=DBDataType.NUMERIC, numeric_precision=10)
    )
    .column(
        column_name="dec_w_precision_and_scale",
        data_type=SqlDataType(name=DBDataType.NUMERIC, numeric_precision=10, numeric_scale=2)
    )
    .build()
)


def test_table_metadata(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    metadata_columns_query: MetadataColumnsQuery = (
        data_source_test_helper.data_source_impl.create_metadata_columns_query()
    )
    get_columns_metadata_query_sql: str = metadata_columns_query.build_sql(
        dataset_prefix=test_table.dataset_prefix,
        dataset_name=test_table.unique_name
    )
    query_result: QueryResult = data_source_test_helper.data_source_impl.execute_query(get_columns_metadata_query_sql)
    metadata_columns_query.get_result(query_result)
