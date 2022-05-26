from __future__ import annotations

import logging
import re

from soda.common.logs import Logs
from soda.common.parser import Parser
from soda.configuration.configuration import Configuration
from soda.sampler.soda_cloud_sampler import SodaCloudSampler
from soda.soda_cloud.soda_cloud import SodaCloud

logger = logging.getLogger(__name__)

DATA_SOURCE = "data_source"
CONNECTION = "connection"


class ConfigurationParser(Parser):
    def __init__(self, configuration: Configuration, logs: Logs, file_path: str):
        super().__init__(file_path=file_path, logs=logs)
        from soda.configuration.configuration import Configuration

        self.configuration: Configuration = configuration

    def parse_environment_yaml_str(self, environment_yaml_str: str):
        environment_yaml_str = self._resolve_jinja(environment_yaml_str)
        environment_dict = self._parse_yaml_str(environment_yaml_str)
        if environment_dict is not None:
            self.__parse_headers(environment_dict)

    def __parse_headers(self, environment_dict: dict) -> None:
        if not environment_dict:
            return

        for environment_header, header_value in environment_dict.items():
            if environment_header.startswith(f"{DATA_SOURCE} "):
                self._push_path_element(environment_header, header_value)
                data_source_name = environment_header[len(f"{DATA_SOURCE} ") :].strip()
                if not re.compile(r"^[a-z_][a-z_0-9]+$").match(data_source_name):
                    self.logs.error(
                        f"Invalid data source name '{data_source_name}'. Data source names must "
                        f"start with a lower case char or an underscore [a-z_], followed by any "
                        f"number of lower case chars, digits or underscore [a-z0-9_]"
                    )
                self.configuration.data_source_properties_by_name[data_source_name] = header_value

                self._get_required("type", str)

                data_source_connection = header_value.get("connection")
                if data_source_connection is None:
                    self.logs.error("connection is required", location=self.location)
                elif isinstance(data_source_connection, dict):
                    connection_name = data_source_name
                    self.configuration.connection_properties_by_name[connection_name] = data_source_connection
                    header_value["connection"] = connection_name
                elif not isinstance(data_source_connection, str):
                    self.logs.error(
                        "connection must be a string or a dict",
                        location=self.location,
                    )
                self._pop_path_element()
            elif environment_header.startswith(f"{CONNECTION} "):
                self._push_path_element(environment_header, header_value)
                connection_name = environment_header[len(f"{CONNECTION} ") :].strip()
                self.configuration.connection_properties_by_name[connection_name] = header_value
                self._pop_path_element()

            elif environment_header == "soda_cloud":
                self._push_path_element("soda_cloud", header_value)
                self.configuration.soda_cloud = self.parse_soda_cloud_cfg(header_value)
                if self.configuration.soda_cloud and not header_value.get("disable_samples"):
                    self.configuration.sampler = SodaCloudSampler()
                self._pop_path_element()

            else:
                self.logs.error(
                    f'Invalid configuration header: expected either "{DATA_SOURCE} {{data source name}}" '
                    f'or "{CONNECTION} {{connection name}}"',
                    location=self.location,
                )

    def parse_soda_cloud_cfg(self, soda_cloud_dict: dict):
        api_key = soda_cloud_dict.get("api_key_id")
        api_secret = soda_cloud_dict.get("api_key_secret")
        host = None
        if "host" in soda_cloud_dict:
            host = soda_cloud_dict.get("host")
        port = None
        if "port" in soda_cloud_dict:
            port = soda_cloud_dict.get("port")
        return SodaCloud(
            api_key_id=api_key, api_key_secret=api_secret, host=host, token=None, port=port, logs=self.logs
        )
