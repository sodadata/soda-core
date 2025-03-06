import sys

from soda_core.common.logs import Logs, Emoticons
from soda_core.common.yaml import YamlSource
from typing import Optional, Dict

if sys.version_info >= (3, 11):
    from typing import Self  # noqa: F401
else:
    from typing_extensions import Self  # noqa: F401


class ContractCommandBuilder:
    def __init__(self):
        self.contract_yaml_sources: list[YamlSource] = []
        self.soda_cloud: Optional['SodaCloud'] = None
        self.soda_cloud_yaml_source: Optional[YamlSource] = None
        self.variables: Optional[Dict[str, str]] = {}
        self.logs: Logs = Logs()
        self.logs.debug("Publishing contract...")

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> Self:
        if isinstance(contract_yaml_file_path, str):
            self.logs.debug(f"  ...with contract file path '{contract_yaml_file_path}'")
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid contract yaml file '{contract_yaml_file_path}'. "
                f"Expected string, but was {contract_yaml_file_path.__class__.__name__}."
            )
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> Self:
        if isinstance(contract_yaml_str, str):
            self.logs.debug(f"  ...with contract YAML str [{len(contract_yaml_str)}]")
            self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str))
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid contract_yaml_str '{contract_yaml_str}'.  "
                f"Expected string, but was {contract_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> Self:
        if isinstance(soda_cloud_yaml_file_path, str):
            if self.soda_cloud_yaml_source is None:
                self.logs.debug(f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_file_path(yaml_file_path=soda_cloud_yaml_file_path)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                f"Expected string, but was {soda_cloud_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> Self:
        if isinstance(soda_cloud_yaml_str, str):
            if self.soda_cloud_yaml_source is None:
                self.logs.debug(f"  ...with soda_cloud_yaml_str [{len(soda_cloud_yaml_str)}]")
            else:
                self.logs.debug(
                    f"{Emoticons.POLICE_CAR_LIGHT} ...with soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                f"Expected string, but was {soda_cloud_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud(self, soda_cloud: object) -> Self:
        self.logs.debug(f"  ...with provided soda_cloud '{soda_cloud}'")
        self.soda_cloud = soda_cloud
        return self

    def with_variable(self, key: str, value: str) -> Self:
        if isinstance(key, str) and isinstance(value, str):
            self.logs.debug(f"  ...with variable '{key}'")
            self.variables[key] = value
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid variable '{key}'. "
                f"Expected key str and value string"
            )
        return self

    def with_variables(self, variables: dict[str, str]) -> Self:
        if isinstance(variables, dict):
            self.logs.debug(f"  ...with variables {list(variables.keys())}")
            self.variables.update(variables)
        elif variables is None:
            if isinstance(self.variables, dict) and len(self.variables) > 1:
                self.logs.debug(f"  ...removing variables {list(self.variables.keys())} because variables set to None")
            self.variables = None
        else:
            self.logs.error(
                f"{Emoticons.POLICE_CAR_LIGHT} ...ignoring invalid variables '{variables}'. "
                f"Expected dict, but was {variables.__class__.__name__}"
            )
        return self
