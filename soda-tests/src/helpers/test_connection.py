from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Optional

import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logs import Logs
from soda_core.common.yaml import DataSourceYamlSource


@dataclass
class TestConnection:
    """
    Define a connection YAML string and test that it can be parsed, used to connect, and run queries.

    You can provide invalid connection parameters and test that an expected error is raised.  See
    example implementation in soda-bigquery/tests/data_sources/test_bigquery.py
    """

    test_name: str
    connection_yaml_str: str
    valid_yaml: Optional[bool] = True
    valid_connection_params: Optional[bool] = True
    query_should_succeed: Optional[bool] = True
    expected_yaml_error: Optional[str] = None
    expected_connection_error: Optional[str] = None
    expected_query_error: Optional[str] = None
    monkeypatches: Optional[dict[str, Any]] = None

    def create_data_source_yaml(self) -> DataSourceYamlSource:
        connection_yaml_str = dedent(self.connection_yaml_str).strip()
        return DataSourceYamlSource.from_str(yaml_str=connection_yaml_str)

    def create_data_source_impl(self, data_source_yaml_source: DataSourceYamlSource) -> DataSourceImpl:
        return DataSourceImpl.from_yaml_source(data_source_yaml_source)

    def test(self, monkeypatch: Optional[pytest.MonkeyPatch] = None):
        if self.monkeypatches:
            for module, mock in self.monkeypatches.items():
                monkeypatch.setattr(module, mock)

        logs = Logs()
        data_source_yaml = self.create_data_source_yaml()
        data_source_impl = None

        try:
            if self.valid_yaml:
                data_source_impl = self.create_data_source_impl(data_source_yaml)
                assert not logs.has_errors
            else:
                with pytest.raises(Exception) as exc_info:
                    self.create_data_source_impl(data_source_yaml)

                assert self.expected_yaml_error in str(exc_info.value)
                # do not try to connect or query if YAML parsing failed
                return

            if self.valid_connection_params:
                data_source_impl.open_connection()
                if logs.has_errors:
                    error_msg = "Connection failed unexpectedly with error: " + logs.get_errors_str()
                    raise RuntimeError(error_msg)
            else:
                data_source_impl.open_connection()
                assert logs.has_errors
                assert self.expected_connection_error in logs.get_errors_str()
                # do not try to query if connection failed
                return

            if self.query_should_succeed:
                data_source_impl.execute_query("SELECT 1")
                if logs.has_errors:
                    error_msg = "Query failed unexpectedly with error: " + logs.get_errors_str()
                    raise RuntimeError(error_msg)
            else:
                with pytest.raises(Exception) as exc_info:
                    data_source_impl.execute_query("SELECT 1")
                assert self.expected_query_error in str(exc_info.value)
                return
        finally:
            if data_source_impl:
                data_source_impl.close_connection()  # safe even if no connection open
