from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logs import Logs
from soda_core.common.yaml import DataSourceYamlSource
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("data_source_api")
    .column_text("id")
    .column_text("country")
    .rows([("1", "US"), ("2", "BE"), ("3", "NL")])
    .build()
)


def test_data_source_api(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_yaml_source: DataSourceYamlSource = data_source_test_helper._create_data_source_yaml_source()
    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(data_source_yaml_source)

    with data_source_impl:
        query_result: QueryResult = data_source_impl.data_source_connection.execute_query(
            f"SELECT * FROM {test_table.qualified_name}"
        )
        assert query_result.rows[0][0] == "1"


def test_empty_data_source_file():
    logs: Logs = Logs()
    data_source_yaml_source: DataSourceYamlSource = DataSourceYamlSource.from_str("")
    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(data_source_yaml_source)
    assert "Data Source YAML string root must be an object, but was empty" in logs.get_errors_str()
