from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_min_max_avg_length(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - min_length(cat) = 3
        - max_length(cat) = 6
        - avg_length(cat) = 4.2
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
