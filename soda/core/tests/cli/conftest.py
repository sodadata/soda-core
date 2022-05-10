from __future__ import annotations

import os

import pytest
from dotenv import load_dotenv
from soda.execution.data_source import DataSource
from tests.conftest import test_data_source

# Load local env file so that test data sources can be set up.
load_dotenv(".env", override=True)


@pytest.fixture(scope="session")
def data_source(data_source_config_str: str) -> DataSource:
    data_source_name = test_data_source

    from soda.scan import Scan

    scan = Scan()
    scan.set_data_source_name(data_source_name)
    scan.add_configuration_yaml_str(data_source_config_str)
    data_source_connection_manager = scan._data_source_manager
    data_source = data_source_connection_manager.get_data_source(data_source_name)
    if not data_source:
        raise Exception(f"Unable to find specified '{data_source_name}' test data_source config.")
    connection = data_source_connection_manager.connect(data_source)
    scan._get_or_create_data_source_scan(test_data_source)

    yield data_source

    connection.close()
