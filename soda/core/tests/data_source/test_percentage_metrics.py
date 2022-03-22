from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_default_missing_percentage(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - missing_percent(pct) = 30
        - invalid_percent(pct) = 10
      configurations for {table_name}:
        missing values for pct: [No value, N/A]
        valid format for pct: percentage
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
