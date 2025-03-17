from __future__ import annotations

import logging
from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.yaml import YamlFileContent, YamlObject

logger: logging.Logger = soda_logger


class DataSourceParser:
    def __init__(self, data_source_yaml_file_content: YamlFileContent, spark_session: Optional[object] = None):
        self.data_source_yaml_file_content: YamlFileContent = data_source_yaml_file_content
        self.spark_session: object = spark_session

    def parse(self) -> Optional[DataSourceImpl]:
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
            logger.error(
                f"Key 'connection' containing an object of data source connection " f"configurations is required"
            )

        format_regexes: dict[str, str] = {}
        format_regexes_yaml: YamlObject = data_source_yaml.read_object_opt("format_regexes")
        if format_regexes_yaml:
            format_regexes_dict: dict = format_regexes_yaml.to_dict()
            for k, v in format_regexes_dict.items():
                if isinstance(k, str) and isinstance(v, str):
                    format_regexes[k] = v
                else:
                    logger.error(
                        msg=f"Invalid regex value in 'format_regexes', " f"expected string, was '{v}'",
                        extra={ExtraKeys.LOCATION: format_regexes_yaml.location},
                    )

        return DataSourceImpl.create(
            data_source_yaml_file_content=self.data_source_yaml_file_content,
            name=data_source_name,
            type_name=data_source_type_name,
            connection_properties=connection_properties,
            format_regexes=format_regexes,
        )
