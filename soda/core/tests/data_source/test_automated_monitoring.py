from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner
from tests.helpers.utils import derive_schema_metric_value_from_test_table


def test_automated_monitoring(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    
    schema_metric_value_derived_from_test_table = derive_schema_metric_value_from_test_table(
        customers_test_table, scanner.data_source
    )

    scan = scanner.create_test_scan()
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name.lower()}-row_count",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name.lower()}-schema",
        metric_values=[schema_metric_value_derived_from_test_table],
    )

    scan.add_sodacl_yaml_str(
        f"""
            automated monitoring:
              tables:
                - include {table_name}
                - exclude PROD%
        """
    )

    scan._is_experimental_auto_monitoring = True
    scan._is_automated_monitoring_run = True
    scan.execute()
