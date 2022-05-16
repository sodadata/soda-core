from __future__ import annotations

import pytest
from tests.helpers.common_test_tables import customers_profiling
from tests.helpers.scanner import Scanner


@pytest.mark.parametrize(
    "table_name, cloud_dict_expectation",
    [
        pytest.param(
            customers_profiling,
            {
                "postgres": {
                    "metadata": [
                        {
                            "table": "sodatest_customers_profiling_bbf94d22",
                            "dataSource": "postgres",
                            "rowCount": 10,
                            "schema": [
                                {"columnName": "id", "sourceDataType": "character varying"},
                                {"columnName": "size", "sourceDataType": "double precision"},
                                {"columnName": "sizetxt", "sourceDataType": "character varying"},
                                {"columnName": "distance", "sourceDataType": "integer"},
                                {"columnName": "pct", "sourceDataType": "character varying"},
                                {"columnName": "cat", "sourceDataType": "character varying"},
                                {"columnName": "country", "sourceDataType": "character varying"},
                                {"columnName": "zip", "sourceDataType": "character varying"},
                                {"columnName": "email", "sourceDataType": "character varying"},
                                {"columnName": "date", "sourceDataType": "date"},
                                {"columnName": "ts", "sourceDataType": "timestamp without time zone"},
                                {"columnName": "ts_with_tz", "sourceDataType": "timestamp with time zone"},
                            ],
                        }
                    ]
                },
                "redshift": {
                    "metadata": [
                        {
                            "table": "sodatest_customers_profiling_bbf94d22",
                            "dataSource": "redshift",
                            "rowCount": 10,
                            "schema": [
                                {"columnName": "id", "sourceDataType": "character varying"},
                                {"columnName": "size", "sourceDataType": "double precision"},
                                {"columnName": "sizetxt", "sourceDataType": "character varying"},
                                {"columnName": "distance", "sourceDataType": "integer"},
                                {"columnName": "pct", "sourceDataType": "character varying"},
                                {"columnName": "cat", "sourceDataType": "character varying"},
                                {"columnName": "country", "sourceDataType": "character varying"},
                                {"columnName": "zip", "sourceDataType": "character varying"},
                                {"columnName": "email", "sourceDataType": "character varying"},
                                {"columnName": "date", "sourceDataType": "date"},
                                {"columnName": "ts", "sourceDataType": "timestamp without time zone"},
                                {"columnName": "ts_with_tz", "sourceDataType": "timestamp with time zone"},
                            ],
                        }
                    ]
                },
                "bigquery": {
                    "metadata": [
                        {
                            "table": "sodatest_customers_profiling_bbf94d22",
                            "dataSource": "bigquery",
                            "rowCount": 10,
                            "schema": [
                                {"columnName": "id", "sourceDataType": "STRING"},
                                {"columnName": "size", "sourceDataType": "NUMERIC"},
                                {"columnName": "sizeTxt", "sourceDataType": "STRING"},
                                {"columnName": "distance", "sourceDataType": "INT64"},
                                {"columnName": "pct", "sourceDataType": "STRING"},
                                {"columnName": "cat", "sourceDataType": "STRING"},
                                {"columnName": "country", "sourceDataType": "STRING"},
                                {"columnName": "zip", "sourceDataType": "STRING"},
                                {"columnName": "email", "sourceDataType": "STRING"},
                                {"columnName": "date", "sourceDataType": "DATE"},
                                {"columnName": "ts", "sourceDataType": "TIMESTAMP"},
                                {"columnName": "ts_with_tz", "sourceDataType": "TIMESTAMP"},
                            ],
                        }
                    ]
                },
                "snowflake": {
                    "metadata": [
                        {
                            "table": "sodatest_customers_profiling_bbf94d22",
                            "dataSource": "snowflake",
                            "rowCount": 10,
                            "schema": [
                                {"columnName": "ID", "sourceDataType": "TEXT"},
                                {"columnName": "SIZE", "sourceDataType": "FLOAT"},
                                {"columnName": "SIZETXT", "sourceDataType": "TEXT"},
                                {"columnName": "DISTANCE", "sourceDataType": "NUMBER"},
                                {"columnName": "PCT", "sourceDataType": "TEXT"},
                                {"columnName": "CAT", "sourceDataType": "TEXT"},
                                {"columnName": "COUNTRY", "sourceDataType": "TEXT"},
                                {"columnName": "ZIP", "sourceDataType": "TEXT"},
                                {"columnName": "EMAIL", "sourceDataType": "TEXT"},
                                {"columnName": "DATE", "sourceDataType": "DATE"},
                                {"columnName": "TS", "sourceDataType": "TIMESTAMP_NTZ"},
                                {"columnName": "TS_WITH_TZ", "sourceDataType": "TIMESTAMP_TZ"},
                            ],
                        }
                    ]
                },
            },
            id="customer table all numric columns",
        )
    ],
)
def test_discover_tables(scanner: Scanner, table_name, cloud_dict_expectation):
    table_name = scanner.ensure_test_table(table_name)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          discover tables:
            tables:
                - include {table_name}
        """
    )
    scan.execute()
    # remove the data source name because it's a pain to test
    discover_tables_result = mock_soda_cloud.scan_result
    assert discover_tables_result is not None
    assert (
        discover_tables_result["metadata"][0]["schema"]
        == cloud_dict_expectation[scan._data_source_name]["metadata"][0]["schema"]
    )
