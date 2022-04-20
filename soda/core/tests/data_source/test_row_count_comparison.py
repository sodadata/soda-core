import textwrap

from tests.helpers.common_test_tables import (
    customers_test_table,
    raw_customers_test_table,
)
from tests.helpers.scanner import Scanner


def test_row_count_comparison(scanner: Scanner):
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    rawcustomers_table_name = scanner.ensure_test_table(raw_customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {customers_table_name}:
        - row_count same as {rawcustomers_table_name}
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_row_count_comparison_cross_data_source(scanner: Scanner, data_source_config_str: str):
    other_data_source_name = "other"
    customers_table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    other_data_source_environment_yaml_str = data_source_config_str.replace("postgres:", f"{other_data_source_name}:")
    other_data_source_environment_yaml_str = textwrap.dedent(other_data_source_environment_yaml_str)
    scan.add_configuration_yaml_str(other_data_source_environment_yaml_str)

    from soda.scan import Scan

    # TODO: refactor this out to be reusable.
    other_scan = Scan()
    other_scan.set_data_source_name(other_data_source_name)
    other_scan.add_configuration_yaml_str(other_data_source_environment_yaml_str)
    other_data_source_connection_manager = other_scan._data_source_manager
    other_data_source = other_data_source_connection_manager.get_data_source(other_data_source_name)
    other_connection = other_data_source_connection_manager.connect(other_data_source)

    other_scanner = Scanner(other_data_source)
    other_scan._get_or_create_data_source_scan(other_data_source_name)
    rawcustomers_table_name = other_scanner.ensure_test_table(raw_customers_test_table)

    scan.add_sodacl_yaml_str(
        f"""
      checks for {customers_table_name}:
        - row_count same as {rawcustomers_table_name} in {other_data_source_name}
    """
    )
    scan.execute()

    other_connection.close()

    scan.assert_all_checks_pass()
