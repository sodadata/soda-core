from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_min_max_avg_sum(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - min(size) = -3
        - max(size) = 6
        - avg(size) between 1.12 and 1.13
        - sum(size) = 7.9
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
