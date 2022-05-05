from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_automated_monitoring(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.mock_historic_values(
      metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name.lower()}-row_count",
      metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0]
    )

    scan.add_sodacl_yaml_str(
        f"""
            automated monitoring:
              tables:
                - include SODATEST_%
                - exclude PROD%
        """
    )

    scan._is_experimental_auto_monitoring = True
    scan._is_automated_monitoring_run = True
    scan.execute()
