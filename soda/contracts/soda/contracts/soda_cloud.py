from __future__ import annotations

import os

from ruamel.yaml import YAML
from yaml import MarkedYAMLError

from soda.contracts.contract import SodaException
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.variable_resolver import VariableResolver


class SodaCloud:
    def __init__(
        self,
        host: str,
        api_key_id: str,
        api_key_secret: str,
        token: str | None,
        port: str | None,
        scheme: str,
        logs: Logs,
    ):
        self.host: str = host
        self.api_key_id: str = api_key_id
        self.api_key_secret: str = api_key_secret
        self.token: str | None = token
        self.port: str | None = port
        self.scheme: str = scheme
        # See also adr/03_exceptions_vs_error_logs.md
        self.logs: Logs = logs

    @classmethod
    def from_environment_variables(cls) -> SodaCloud:
        return SodaCloud.from_dict({})

    @classmethod
    def from_yaml_file(cls, soda_cloud_yaml_file_path: str) -> SodaCloud:
        """
        soda_cloud: SodaCloud = SodaCloud.from_yaml_file(file_path)

        :param soda_cloud_yaml_file_path: A file path to a YAML file containing the Soda Cloud configuration
            properties. See TODO for more details on the schema of the YAML file

        :return: A Soda Cloud object containing the Soda Cloud connection properties
        :raises SodaCloudException: if SodaCloud cannot be created for any reason
        """
        logs: Logs = Logs()

        try:
            if not isinstance(soda_cloud_yaml_file_path, str):
                logs.error(
                    f"Couldn't create SodaCloud from yaml file. Expected str in parameter "
                    f"soda_cloud_yaml_file_path={soda_cloud_yaml_file_path}, but was '{type(soda_cloud_yaml_file_path)}"
                )
            if not len(soda_cloud_yaml_file_path) > 1:
                logs.error(f"Couldn't create SodaCloud from yaml file. soda_cloud_yaml_file_path is an empty string")

            with open(file=soda_cloud_yaml_file_path) as f:
                soda_cloud_yaml_str = f.read()
                return cls.from_yaml_str(soda_cloud_yaml_str=soda_cloud_yaml_str, logs=logs)
        except Exception as e:
            raise SodaException(f"Couldn't create SodaCloud from yaml file '{soda_cloud_yaml_file_path}'") from e

    @classmethod
    def from_yaml_str(cls, soda_cloud_yaml_str: str, logs: Logs | None = None) -> SodaCloud:
        """
        soda_cloud_yaml_str: str = "...YAML string for Soda Cloud configuration properties..."
        soda_cloud: SodaCloud = SodaCloud.from_yaml_str(soda_cloud_yaml_str) as connection:
        # do stuff with Soda Cloud.

        More docs on SodaCloud: see ../../docs/creating_a_soda_cloud.md
        """

        if not logs:
            logs = Logs()

        if not isinstance(soda_cloud_yaml_str, str):
            logs.error(
                f"Expected a string for parameter soda_cloud_yaml_str, " f"but was '{type(soda_cloud_yaml_str)}'"
            )

        if soda_cloud_yaml_str == "":
            logs.error(f"soda_cloud_yaml_str must be non-emtpy, but was ''")

        variable_resolver = VariableResolver(logs=logs)
        resolved_soda_cloud_yaml_str: str = variable_resolver.resolve(soda_cloud_yaml_str)

        soda_cloud_dict: object | None = None
        try:
            yaml = YAML()
            yaml.preserve_quotes = True
            soda_cloud_dict = yaml.load(resolved_soda_cloud_yaml_str)
        except MarkedYAMLError as e:
            mark = e.context_mark if e.context_mark else e.problem_mark
            line = (mark.line + 1,)
            column = (mark.column + 1,)
            logs.error(f"YAML syntax error: {e} | line={line} | column={column}")

        if not isinstance(soda_cloud_dict, dict):
            logs.error(f"Content of the SodaCloud YAML file must be a YAML object, " f"but was {type(soda_cloud_dict)}")

        return cls.from_dict(soda_cloud_dict=soda_cloud_dict, logs=logs)

    @classmethod
    def from_dict(cls, soda_cloud_dict: dict | None = None, logs: Logs | None = None) -> SodaCloud | None:
        if soda_cloud_dict is None:
            soda_cloud_dict = {}

        if not logs:
            logs = Logs()

        if not isinstance(soda_cloud_dict, dict):
            logs.error(f"soda_cloud_dict must be a dict, but was {type(soda_cloud_dict)}")

        host: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict, key="host", logs=logs, default_value="cloud.soda.io"
        )
        api_key_id: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict, key="api_key_id", logs=logs, required=True
        )
        api_key_secret: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict, key="api_key_secret", logs=logs, required=True
        )
        token: str | None = cls.__get_configuration_value(soda_cloud_dict=soda_cloud_dict, key="token", logs=logs)
        port: str | None = cls.__get_configuration_value(soda_cloud_dict=soda_cloud_dict, key="port", logs=logs)
        scheme: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict, key="scheme", logs=logs, default_value="https"
        )
        if not isinstance(api_key_id, str) or not isinstance(api_key_secret, str):
            return None
        return SodaCloud(
            host=host,
            api_key_id=api_key_id,
            api_key_secret=api_key_secret,
            token=token,
            port=port,
            scheme=scheme,
            logs=logs,
        )

    @classmethod
    def __get_configuration_value(
        cls,
        soda_cloud_dict: dict,
        key: str,
        logs: Logs,
        default_value: str | None = None,
        required: bool = False,
    ) -> str:
        environment_key_lower = f"soda_cloud_{key}".lower()
        if environment_key_lower in os.environ:
            return os.environ[environment_key_lower]
        environment_key_upper = environment_key_lower.upper()
        if environment_key_upper in os.environ:
            return os.environ[environment_key_upper]
        if key in soda_cloud_dict:
            return soda_cloud_dict[key]
        if not required:
            return default_value
        logs.error(f"Key {environment_key_lower} not found in Soda Cloud configuration")
