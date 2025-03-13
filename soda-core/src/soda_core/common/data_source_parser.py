from __future__ import annotations

from typing import Optional

from soda_core.common.data_source import DataSource
from soda_core.common.logs import Emoticons, Logs
from soda_core.common.yaml import YamlFileContent, YamlObject


class DataSourceParser:
    def __init__(self, data_source_yaml_file_content: YamlFileContent, spark_session: Optional[object] = None):
        self.data_source_yaml_file_content: YamlFileContent = data_source_yaml_file_content
        self.logs: Logs = data_source_yaml_file_content.logs
        self.spark_session: object = spark_session

    def parse(self) -> Optional[DataSource]:
        if not self.data_source_yaml_file_content:
            return None

        data_source_yaml: YamlObject = self.data_source_yaml_file_content.get_yaml_object()
        if not data_source_yaml:
            return None

        data_source_type_name: str = data_source_yaml.read_string("type")
        data_source_name: Optional[str] = data_source_yaml.read_string("name")

        connection_yaml: YamlObject = data_source_yaml.read_object_opt("connection")
        connection_properties: Optional[dict] = None
        if connection_yaml:
            connection_properties = connection_yaml.to_dict()
        elif self.spark_session is None:
            self.logs.error(
                f"Key 'connection' containing an object of data source connection "
                f"configurations is required"
            )

        format_regexes: dict[str, str] = {}
        format_regexes_yaml: YamlObject = data_source_yaml.read_object_opt("format_regexes")
        if format_regexes_yaml:
            format_regexes_dict: dict = format_regexes_yaml.to_dict()
            for k, v in format_regexes_dict.items():
                if isinstance(k, str) and isinstance(v, str):
                    format_regexes[k] = v
                else:
                    self.logs.error(
                        message=f"Invalid regex value in 'format_regexes', "
                        f"expected string, was '{v}'",
                        location=format_regexes_yaml.location,
                    )

        return DataSource.create(
            data_source_yaml_file_content=self.data_source_yaml_file_content,
            name=data_source_name,
            type_name=data_source_type_name,
            connection_properties=connection_properties,
            format_regexes=format_regexes,
        )
