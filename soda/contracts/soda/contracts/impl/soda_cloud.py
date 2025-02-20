from __future__ import annotations

import os

from soda.contracts.impl.yaml_helper import YamlFile, YamlHelper


class SodaCloud:
    def __init__(self, soda_cloud_file: YamlFile):
        logs = soda_cloud_file.logs
        configuration_dict = soda_cloud_file.get_dict() if soda_cloud_file.is_ok() else {}
        yaml_helper: YamlHelper = YamlHelper(logs=logs, yaml_file=soda_cloud_file)

        def get_configuration(key: str, default_value: str | None = None, is_required: bool = True) -> str | None:
            """
            Looks for the key in the configuration_dict, if it exists
            If not, in the corresponding environment variable
            If not applies the default value
            """
            environment_variable_name: str = f"SODA_CLOUD_{key.upper()}"
            default_value = os.environ.get(environment_variable_name, default_value)
            value = yaml_helper.read_string_opt(d=configuration_dict, key=key, default_value=default_value)
            if is_required and not isinstance(value, str):
                logs.error(f"Soda Cloud configuration '{key}' not provided as configuration nor environment variable")
            return value

        self.host: str = get_configuration(key="host", default_value="cloud.soda.io")
        self.api_key_id: str = get_configuration(key="api_key_id")
        self.api_key_secret: str = get_configuration(key="api_key_secret")
        self.token: str | None = get_configuration(key="token", is_required=False)
        self.port: str | None = get_configuration(key="port", is_required=False)
        self.scheme: str = get_configuration(key="scheme", is_required=False)
