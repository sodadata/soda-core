import os

import pytest
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.data_source_fixture import DataSourceFixture
from tests.helpers.utils import derive_schema_metric_value_from_test_table


@pytest.mark.skipif(
    condition=os.getenv("SCIENTIFIC_TESTS") == "SKIP",
    reason="Environment variable SCIENTIFIC_TESTS is set to SKIP which skips tests depending on the scientific package",
)
def test_automated_monitoring(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    table_name = data_source_fixture.data_source.default_casify_table_name(table_name)

    schema_metric_value_derived_from_test_table = derive_schema_metric_value_from_test_table(
        customers_test_table, data_source_fixture.data_source
    )

    scan = data_source_fixture.create_test_scan()
    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-row_count-automated_monitoring",
        metric_values=[10, 10, 10, 9, 8, 8, 8, 0, 0, 0],
    )

    scan.mock_historic_values(
        metric_identity=f"metric-{scan._scan_definition_name}-{scan._data_source_name}-{table_name}-schema-automated_monitoring",
        metric_values=[schema_metric_value_derived_from_test_table],
    )

    scan.add_sodacl_yaml_str(
        f"""
            automated monitoring:
              datasets:
                - include {table_name}
                - exclude PROD%
        """
    )
    scan.execute()
