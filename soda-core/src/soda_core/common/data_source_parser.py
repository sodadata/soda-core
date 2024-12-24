from __future__ import annotations

from soda_core.common.data_source import DataSource
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource, YamlObject


class DataSourceParser:

    def __init__(
            self,
            data_source_yaml_file: YamlSource,
            spark_session: object | None = None
    ):
        self.logs: Logs = data_source_yaml_file.logs
        self.data_source_yaml_file: YamlSource = data_source_yaml_file
        self.spark_session: object = spark_session

    def parse(self, variables: dict | None = None) -> DataSource | None:
        self.data_source_yaml_file.parse(variables)
        data_source_yaml: YamlObject = self.data_source_yaml_file.get_yaml_object()
        if data_source_yaml:
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
                data_source_yaml_file=self.data_source_yaml_file,
                name=data_source_name,
                type_name=data_source_type_name,
                connection_properties=connection_properties,
                spark_session=self.spark_session
            )
