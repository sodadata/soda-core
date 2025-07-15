import os
from typing import Optional
from textwrap import dedent
from dataclasses import dataclass
import pytest
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import DataSourceYamlSource
from soda_core.common.logs import Logs

@dataclass
class BigQueryTestConnection:
    test_name: str
    connection_yaml_str: str
    valid_yaml: bool
    valid_connection_params: bool
    query_should_succeed: bool
    expected_yaml_error: Optional[str] = None
    expected_connection_error: Optional[str] = None
    expected_query_error: Optional[str] = None

    def create_data_source_yaml(self) -> DataSourceYamlSource:
        connection_yaml_str = dedent(self.connection_yaml_str).strip()
        return DataSourceYamlSource.from_str(yaml_str=connection_yaml_str)

    def create_data_source_impl(self, data_source_yaml_source: DataSourceYamlSource) -> DataSourceImpl:
        return DataSourceImpl.from_yaml_source(data_source_yaml_source)
  
    
    
test_connections: list[BigQueryTestConnection] = [    
        BigQueryTestConnection(
            test_name="connection_basic",
            connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "")}'
                    location: '{os.getenv("BIGQUERY_LOCATION", "US")}'
            """,
            valid_yaml=True,
            valid_connection_params=True,
            query_should_succeed=True,
        ),
        BigQueryTestConnection(
            test_name="connection_basic_bad_yaml",
            connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_jsosn: '{os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "")}'
                    location: '{os.getenv("BIGQUERY_LOCATION", "US")}'
            """,
            valid_yaml=False,
            expected_yaml_error="""BigQueryJSONStringAuth.account_info_json\n  Field required [type=missing, input_value={\'account_info_jsosn\'""",
            valid_connection_params=True,
            expected_connection_error=None,
            query_should_succeed=True,
            expected_query_error=None
        ),
        BigQueryTestConnection(
            test_name="connection_basic_bad_credentials",
            connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: 'BAD_CREDENTIALS'
                    location: '{os.getenv("BIGQUERY_LOCATION", "US")}'
            """,
            valid_yaml=True,
            valid_connection_params=False,
            expected_connection_error="Could not connect to 'BIGQUERY_TEST_DS'",
            query_should_succeed=False            
        ),
        BigQueryTestConnection(
            test_name="connection_basic_bad_location",
            connection_yaml_str=f"""
                type: bigquery
                name: BIGQUERY_TEST_DS
                connection:
                    account_info_json: '{os.getenv("BIGQUERY_ACCOUNT_INFO_JSON", "")}'
                    location: '{os.getenv("BIGQUERY_LOCATION", "XX")}'
            """,
            valid_yaml=True,
            valid_connection_params=True,
            query_should_succeed=False,
            expected_query_error="Location XX does not support this operation",
        )
        ]


class TestBigQueryAuth:

    @pytest.mark.parametrize("test_connection", test_connections, ids=[tc.test_name for tc in test_connections])
    def test_connection_scenarios(self, test_connection: BigQueryTestConnection):
        logs = Logs()
        data_source_yaml = test_connection.create_data_source_yaml()
        if test_connection.valid_yaml:
            data_source_impl = test_connection.create_data_source_impl(data_source_yaml)
            assert not logs.has_errors()
        else:
            with pytest.raises(Exception) as exc_info:
                test_connection.create_data_source_impl(data_source_yaml)

            assert test_connection.expected_yaml_error in str(exc_info.value)
            return 

        if test_connection.valid_connection_params:
            data_source_impl.open_connection()
            assert not logs.has_errors()

        else:
            data_source_impl.open_connection()
            assert logs.has_errors()
            assert test_connection.expected_connection_error in logs.get_errors_str()
            return


        if test_connection.query_should_succeed:
            data_source_impl.execute_query("SELECT 1")
            assert not logs.has_errors()            
        else:
            with pytest.raises(Exception) as exc_info:
                data_source_impl.execute_query("SELECT 1")
            assert test_connection.expected_query_error in str(exc_info.value)
            return

        data_source_impl.close_connection()
