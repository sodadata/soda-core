from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_profile_columns(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: ['%{table_name}%.%']
        """
    )
    scan.execute()
