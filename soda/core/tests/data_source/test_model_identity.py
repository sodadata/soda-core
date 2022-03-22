from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner

# Tests for testing that check and metric identity are calculated and compared correctly, that no conflicts occur.
# TODO: add more tests that are similar/close enough and could break the identity logic.


def test_metric_identity(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - failed rows:
            name: Size cannot be zero
            fail condition: size = 0
        - failed rows:
            name: All records must be from this millennium
            fail query: |
                select * from {table_name} where date < '2000-01-01'
        - failed rows:
            name: Size absolute must be 1M max
            fail query: |
                select * from {table_name} where ABS(size) > 1000000
        - size_dst:
            name: Absolute average size and distance product must be < 1000000
            size_dst expression: ABS(AVG(size * distance))
            warn: when > 1000000
            fail: when > 1500000
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
