from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.mock_soda_cloud import MockResponse
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("soda_cloud")
    .column_text("id")
    .column_integer("age")
    .rows(rows=[
        ("1",  1),
        (None, -1),
        ("3",  None),
        ("X",  2),
    ])
    .build()
)


def test_soda_cloud_results(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.set_mock_soda_cloud_responses([MockResponse(
        status_code=200,
        json_dict={"fileId": "777ggg"}
    )])

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - type: missing_count
                    must_be_less_than_or_equal: 2
        """
    )


def test_execute_over_agent(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.set_mock_soda_cloud_responses([MockResponse(
        status_code=200,
        json_dict={"fileId": "777ggg"}
    )])

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [-1, -2]
                checks:
                  - type: missing_count
                    must_be_less_than_or_equal: 2
        """
    )
