from __future__ import annotations

from soda.execution.data_type import DataType
from tests.helpers.common_test_tables import (
    customers_dist_check_test_table,
    customers_profiling,
    customers_test_table,
    orders_test_table,
)
from tests.helpers.data_source_fixture import DataSourceFixture


def test_discover_tables(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_profiling)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          discover datasets:
            datasets:
                - include {table_name}
        """
    )
    scan.execute(allow_warnings_only=True)
    # remove the data source name because it's a pain to test
    discover_tables_result = mock_soda_cloud.pop_scan_result()

    assert discover_tables_result is not None
    actual_metadatas = discover_tables_result["metadata"]
    actual_metadata = actual_metadatas[0]
    actual_schema = actual_metadata["schema"]

    data_source = data_source_fixture.data_source
    to_ds_type = data_source.get_sql_type_for_schema_check
    to_ds_case = data_source.default_casify_column_name

    expected_schema = [
        {"columnName": to_ds_case("id"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("size"), "sourceDataType": to_ds_type(DataType.DECIMAL)},
        {"columnName": to_ds_case("sizeTxt"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("distance"), "sourceDataType": to_ds_type(DataType.INTEGER)},
        {"columnName": to_ds_case("pct"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("cat"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("country"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("zip"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("email"), "sourceDataType": to_ds_type(DataType.TEXT)},
        {"columnName": to_ds_case("date"), "sourceDataType": to_ds_type(DataType.DATE)},
        {"columnName": to_ds_case("ts"), "sourceDataType": to_ds_type(DataType.TIMESTAMP)},
        {"columnName": to_ds_case("ts_with_tz"), "sourceDataType": to_ds_type(DataType.TIMESTAMP_TZ)},
    ]

    assert actual_schema == expected_schema


def test_discover_tables_customer_wildcard(data_source_fixture: DataSourceFixture):
    
    data_source_fixture.ensure_test_table(orders_test_table)
    data_source_fixture.ensure_test_table(customers_profiling)
    data_source_fixture.ensure_test_table(customers_dist_check_test_table)

    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    table_name = data_source_fixture.data_source.default_casify_table_name(table_name)
    wildcard = f"%{table_name.split('_')[1]}%"

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
        discover datasets:
          datasets:
            - include {wildcard}
        """
    )
    scan.execute(allow_warnings_only=True)
    discover_tables_result = mock_soda_cloud.pop_scan_result()
    assert discover_tables_result is not None
    assert len(discover_tables_result["metadata"]) == 3
