from textwrap import dedent

from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.mock_file_system import MockFileSystem


def test_parsing_complete_and_correct_producer_contract_file(
    data_source_fixture: DataSourceFixture, mock_file_system: MockFileSystem
):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    contract_file_path = f"{mock_file_system.user_home_dir()}/customers.yml"

    mock_file_system.files = {
        contract_file_path: dedent(
            f"""
            datasource: {data_source_fixture.data_source_name}
            name: {table_name}
            label: Customers
            schema:
              id:
              distance:
              cat:
            checks:
              - row_count > 0
            """
        ).strip()
    }

    scan = data_source_fixture.create_test_scan()
    scan.add_contract_yaml_file(contract_file_path)

    scan.assert_no_error_logs()

    scan.execute()

    scan.assert_no_error_logs()
