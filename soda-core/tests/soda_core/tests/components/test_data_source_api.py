from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import QueryResult
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.yaml import YamlFile
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification


test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("data_source_api")
    .column_text("id")
    .column_text("country")
    .rows([
        ('1', 'US'),
        ('2', 'BE'),
        ('3', 'NL')
    ])
    .build()
)


def test_data_source_api(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_yaml_dict: dict = data_source_test_helper._create_data_source_yaml_dict()
    data_source_yaml_file: YamlFile = YamlFile(yaml_dict=data_source_yaml_dict)
    data_source_parser: DataSourceParser = DataSourceParser(data_source_yaml_file)
    data_source: DataSource = data_source_parser.parse()

    with data_source:
        query_result: QueryResult = data_source.data_source_connection.execute_query(
            f"SELECT * FROM {test_table.qualified_name}"
        )
        assert query_result.rows[0][0] == "1"
