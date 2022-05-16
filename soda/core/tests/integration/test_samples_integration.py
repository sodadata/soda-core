import os
from textwrap import dedent

import pytest
from soda.scan import Scan
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


@pytest.mark.skipif(
    os.getenv("INTEGRATION", None) != "ENABLED",
    reason="Run only if integration tests are enabled.",
)
def test_scan(scanner: Scanner, data_source_config_str: str):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = Scan()
    scan.set_verbose()
    scan.set_scan_definition_name("test_samples_integration")
    scan.add_configuration_yaml_str(data_source_config_str)
    scan.set_data_source_name("postgres")

    scan.add_configuration_yaml_str(
        dedent(
            f"""
              soda_cloud:
                api_key_id: ${{DEV_SODADATA_IO_API_KEY_ID}}
                api_key_secret: ${{DEV_SODADATA_IO_API_KEY_SECRET}}
                host: dev.sodadata.io
            """
        )
    )

    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(cat) > 0
        """
    )
    scan.execute()
