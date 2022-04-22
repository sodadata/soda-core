import pytest
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


@pytest.mark.skip
def test_automated_monitoring(scanner: Scanner):
    scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()

    scan.mock_historic_values(metric_identity="x", metric_values=[])

    scan.add_sodacl_yaml_str(
        f"""
            automated monitoring:
              tables:
                - include SODATEST_%
                - exclude PROD%
              find anomalies:
                row count: true
                schema: true
        """
    )
    scan.execute()
