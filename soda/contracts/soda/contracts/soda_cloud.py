from __future__ import annotations

import os

from ruamel.yaml import YAML
from yaml import MarkedYAMLError

from soda.contracts.connection import SodaException
from soda.contracts.impl.variable_resolver import VariableResolver


class SodaCloud:
    def __init__(self, host: str, api_key_id: str, api_key_secret: str, scheme: str = "https"):
        self.host: str = host
        self.api_key_id: str = api_key_id
        self.api_key_secret: str = api_key_secret
        self.scheme: str = scheme

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
        try:
            if not isinstance(soda_cloud_yaml_file_path, str):
                raise SodaException(
                    f"Couldn't create SodaCloud from yaml file. Expected str in parameter "
                    f"soda_cloud_yaml_file_path={soda_cloud_yaml_file_path}, but was '{type(soda_cloud_yaml_file_path)}"
                )
            if not len(soda_cloud_yaml_file_path) > 1:
                raise SodaException(
                    f"Couldn't create SodaCloud from yaml file. soda_cloud_yaml_file_path is an empty string"
                )

            with open(file=soda_cloud_yaml_file_path) as f:
                soda_cloud_yaml_str = f.read()
                return cls.from_yaml_str(soda_cloud_yaml_str)
        except Exception as e:
            raise SodaException(
                f"Couldn't create SodaCloud from yaml file '{soda_cloud_yaml_file_path}'"
            ) from e

    @classmethod
    def from_yaml_str(cls, soda_cloud_yaml_str: str) -> SodaCloud:
        """
        soda_cloud_yaml_str: str = "...YAML string for Soda Cloud configuration properties..."
        soda_cloud: SodaCloud = SodaCloud.from_yaml_str(soda_cloud_yaml_str) as connection:
        # do stuff with Soda Cloud.

        More docs on SodaCloud: see ../../docs/creating_a_soda_cloud.md
        """

        if not isinstance(soda_cloud_yaml_str, str):
            raise SodaException(
                f"Expected a string for parameter soda_cloud_yaml_str, "
                f"but was '{type(soda_cloud_yaml_str)}'"
            )

        if soda_cloud_yaml_str == "":
            raise SodaException(
                f"soda_cloud_yaml_str must be non-emtpy, but was ''"
            )

        variable_resolver = VariableResolver()
        resolved_soda_cloud_yaml_str: str = variable_resolver.resolve(soda_cloud_yaml_str)
        # variable_resolver.logs.assert_no_errors()

        try:
            yaml = YAML()
            yaml.preserve_quotes = True
            soda_cloud_dict = yaml.load(resolved_soda_cloud_yaml_str)
            if not isinstance(soda_cloud_dict, dict):
                raise SodaException(
                    f"Content of the SodaCloud YAML file must be a YAML object, "
                    f"but was {type(soda_cloud_dict)}"
                )
            return cls.from_dict(soda_cloud_dict)
        except MarkedYAMLError as e:
            mark = e.context_mark if e.context_mark else e.problem_mark
            line = mark.line + 1,
            column = mark.column + 1,
            raise SodaException(f"YAML syntax error: {e} | line={line} | column={column}")

    @classmethod
    def from_dict(cls, soda_cloud_dict: dict | None = None) -> SodaCloud:
        if soda_cloud_dict is None:
            soda_cloud_dict = {}

        if not isinstance(soda_cloud_dict, dict):
            raise SodaException(f"soda_cloud_dict must be a dict, but was {type(soda_cloud_dict)}")

        host: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict,
            key="host",
            default_value="cloud.soda.io"
        )
        api_key_id: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict,
            key="api_key_id"
        )
        api_key_secret: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict,
            key="api_key_secret"
        )
        scheme: str = cls.__get_configuration_value(
            soda_cloud_dict=soda_cloud_dict,
            key="scheme",
            default_value="https"
        )
        return SodaCloud(host=host, api_key_id=api_key_id, api_key_secret=api_key_secret, scheme=scheme)

    @classmethod
    def __get_configuration_value(cls, soda_cloud_dict: dict, key: str, default_value: str | None = None) -> str:
        environment_key_lower = f"soda_cloud_{key}".lower()
        if environment_key_lower in os.environ:
            return os.environ[environment_key_lower]
        environment_key_upper = environment_key_lower.upper()
        if environment_key_upper in os.environ:
            return os.environ[environment_key_upper]
        if key in soda_cloud_dict:
            return soda_cloud_dict[key]
        if default_value is not None:
            return default_value
        raise SodaException(f"Key {environment_key_lower} not found in Soda Cloud configuration")
