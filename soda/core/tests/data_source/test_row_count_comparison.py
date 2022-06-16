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


def test_row_count_comparison_cross_data_source(scanner: Scanner):
    """Does not really create two connections and test cross data sources, that is handled in integration tests.

    Tests syntax parsing and check execution.
    """
    customers_table_name = scanner.ensure_test_table(customers_test_table)
    rawcustomers_table_name = scanner.ensure_test_table(raw_customers_test_table)

    # Reuse the same data source name
    other_data_source_name = scanner.data_source.data_source_name

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
            checks for {customers_table_name}:
              - row_count same as {rawcustomers_table_name} in {other_data_source_name}
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
