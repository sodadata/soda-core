from unittest import TestCase

import pytest
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture

mock_schema = [
    {"type": "number", "name": "priority"},
    {"type": "singleSelect", "allowedValues": ["sales", "marketing"], "name": "department"},
    {"type": "multiSelect", "allowedValues": ["generated", "user-created"], "name": "tags"},
    {"type": "text", "name": "sales_owner"},
    {"type": "datetime", "name": "arrival_date"},
    {"type": "datetime", "name": "arrival_datetime"},
]
mock_variables = {"DEPT": "sales"}


@pytest.mark.skip(reason="Enable once attribute validation is actually used in scan.")
def test_check_attributes_valid(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_soda_cloud()
    scan.mock_check_attributes_schema(mock_schema)
    scan.add_variables(mock_variables)
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - row_count > 0:
            attributes:
                priority: 1
                department: ${{DEPT}}
                ${{DEPT}}_owner: John Doe
                tags: ["user-created"]
                arrival_date: "2022-12-12"
                arrival_datetime: "2022-12-12T12:00:00"
    """
    )
    scan.execute()
    scan.assert_all_checks_pass()

    scan_result = scan.build_scan_results()
    TestCase().assertDictEqual(
        {
            "priority": 1,
            "department": "sales",
            "sales_owner": "John Doe",
            "tags": ["user-created"],
            "arrival_date": "2022-12-12",
            "arrival_datetime": "2022-12-12T12:00:00",
        },
        scan_result["checks"][0]["attributes"],
    )


@pytest.mark.skip(reason="Enable once attribute validation is actually used in scan.")
def test_check_attributes_invalid(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_soda_cloud()
    scan.mock_check_attributes_schema(mock_schema)
    scan.add_variables(mock_variables)
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - row_count > 0:
            attributes:
                priority: "high"
                something-invalid: some-value
                tags: ["unknown"]
                arrival_date: 2022/01/01
                arrival_datetime: 2022/01/01T01:01:01

    """
    )
    scan.execute_unchecked()

    scan_result = scan.build_scan_results()
    assert scan_result["checks"][0]["attributes"] == {}

    scan.assert_has_error(
        "Soda Cloud does not recognize 'tags': '['unknown']' attribute value. Valid attribute values: ['generated', 'user-created']"
    )
    scan.assert_has_error(
        "Soda Cloud does not recognize 'DoubleQuotedScalarString' type of attribute 'priority'. It expects the following type(s): ['int', 'float']"
    )
    scan.assert_has_error("Soda Cloud does not recognize 'something-invalid' attribute name.")
    scan.assert_has_error(
        "Soda Cloud expects an ISO formatted date or datetime value for the 'arrival_date' attribute."
    )
    scan.assert_has_error(
        "Soda Cloud expects an ISO formatted date or datetime value for the 'arrival_datetime' attribute."
    )


@pytest.mark.skip(reason="Enable once attribute validation is actually used in scan.")
def test_foreach_attributes(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.enable_mock_soda_cloud()
    scan.mock_check_attributes_schema(mock_schema)
    scan.add_variables(mock_variables)
    scan.add_sodacl_yaml_str(
        f"""
      for each dataset D:
        datasets:
              - {table_name}
        checks:
        - row_count > 0:
            attributes:
                priority: 1
                tags: ["generated"]
                department: ${{DEPT}}
                ${{DEPT}}_owner: John Doe

    """
    )
    scan.execute()
    scan.assert_all_checks_pass()

    scan_result = scan.build_scan_results()
    TestCase().assertDictEqual(
        {
            "priority": 1,
            "tags": ["generated"],
            "department": "sales",
            "sales_owner": "John Doe",
        },
        scan_result["checks"][0]["attributes"],
    )
