from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource, YamlObject, YamlFileContent


class DataSourceParser:

    def __init__(
            self,
            data_source_yaml_source: YamlSource,
            logs: Logs | None = None,
            spark_session: object | None = None
    ):
        self.logs: Logs = logs if logs else Logs()
        self.data_source_yaml_source: YamlSource = data_source_yaml_source
        self.spark_session: object = spark_session

    def parse(self, variables: dict | None = None) -> DataSource | None:
        yaml_files_content: YamlFileContent = self.data_source_yaml_source.parse_yaml_file_content(
            file_type="data source", variables=variables, logs=self.logs
        )
        if not yaml_files_content:
            return None

        data_source_yaml: YamlObject = yaml_files_content.get_yaml_object()
        if not data_source_yaml:
            return None

        data_source_type_name: str = data_source_yaml.read_string("type")
        data_source_name: str | None = data_source_yaml.read_string("name")

        connection_yaml: YamlObject = data_source_yaml.read_object_opt("connection")
        connection_properties: dict | None = None
        if connection_yaml:
            connection_properties = connection_yaml.to_dict()
        elif self.spark_session is None:
            self.logs.error(
                "Key 'connection' containing an object of data source connection configurations is required"
                # TODO add location
            )

        return DataSource.create(
            data_source_yaml_source=self.data_source_yaml_source,
            name=data_source_name,
            type_name=data_source_type_name,
            connection_properties=connection_properties,
            variables=variables
        )
