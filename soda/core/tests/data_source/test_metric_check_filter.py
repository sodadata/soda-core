from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_count_filtered(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    # Row count is 10
    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - row_count = 3:
            filter: cat = 'HIGH'
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_missing_filtered(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    # Row count is 10
    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - missing_count(pct) = 1:
            missing values: [No value, N/A, error]
            filter: cat = 'HIGH'
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_valid_filtered(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    # Row count is 10
    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - valid_count(pct) = 2:
                valid format: percentage
                missing values: [No value, N/A, error]
                filter: cat = 'HIGH'
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_valid_percentage_filtered(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    # Row count is 10
    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - invalid_percent(pct) = 40:
            valid format: percentage
            missing values: [N/A]
            filter: cat IS NULL
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
