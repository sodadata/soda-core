from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_duplicates_single_column(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - duplicate_count(cat) = 1
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_duplicates_multiple_columns(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - duplicate_count(cat, country) = 1
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
