import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import DataSourceYamlSource

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("query_result_iterator")
    .column_varchar("id", character_maximum_length=255)  # max length required for sql server
    .column_varchar("country", character_maximum_length=255)  # max length required for sql server
    .rows([("1", "US"), ("2", "BE"), ("3", "NL")])
    .build()
)


@pytest.mark.no_snapshot
def test_data_source_query_result_iterator(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_yaml_source: DataSourceYamlSource = data_source_test_helper._create_data_source_yaml_source()
    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(data_source_yaml_source)

    with data_source_impl:
        # Order by required since some DB engines don't guarantee row order
        id_quoted = data_source_test_helper.quote_column("id")
        with data_source_impl.execute_query_iterate(
            f"SELECT * FROM {test_table.qualified_name} ORDER BY {id_quoted}"
        ) as query_result_iterator:
            # Oracle returns the number of rows that have been read up until this point
            if data_source_impl.type_name == "oracle":
                expected_row_count = 0
            # These databases don't determine the number of rows and return -1
            elif data_source_impl.type_name in (
                "duckdb",
                "sqlserver",
                "fabric",
                "synapse",
                "trino",
                "databricks",
                "athena",
                "dremio",
                "db2",
                # HANA's hdbcli reports rowcount=1 after a fetchall() but
                # rowcount=-1 from the streaming iterator path before the
                # first row is fetched (the cursor doesn't know the
                # cardinality up front).
                "hana",
            ):
                expected_row_count = -1
            # Other datasources should correctly return row count
            else:
                expected_row_count = 3

            assert query_result_iterator.row_count == expected_row_count
            assert query_result_iterator.columns.keys() == {"id", "country"}

            rows = list(query_result_iterator)
            assert len(rows) == 3
            assert rows[0][0] == "1"
            assert rows[1][0] == "2"
            assert rows[2][0] == "3"
