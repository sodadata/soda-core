import logging
from typing import Dict, Optional, TypeVar

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlSource

T = TypeVar("T", bound="ContractCommandBuilder")


logger: logging.Logger = soda_logger


class ContractCommandBuilder:
    def __init__(self, logs: Optional[Logs] = None):
        self.contract_yaml_sources: list[YamlSource] = []
        self.soda_cloud: Optional["SodaCloud"] = None
        self.soda_cloud_yaml_source: Optional[YamlSource] = None
        self.variables: Optional[Dict[str, str]] = {}
        self.logs: Logs = logs if logs else Logs()
        logger.debug("Publishing contract...")

    def with_contract_yaml_file(self, contract_yaml_file_path: str) -> T:
        if isinstance(contract_yaml_file_path, str):
            logger.debug(f"  ...with contract file path '{contract_yaml_file_path}'")
            self.contract_yaml_sources.append(YamlSource.from_file_path(yaml_file_path=contract_yaml_file_path))
        else:
            logger.error(
                f"...ignoring invalid contract yaml file '{contract_yaml_file_path}'. "
                f"Expected string, but was {contract_yaml_file_path.__class__.__name__}."
            )
        return self

    def with_contract_yaml_str(self, contract_yaml_str: str) -> T:
        if isinstance(contract_yaml_str, str):
            logger.debug(f"  ...with contract YAML str [{len(contract_yaml_str)}]")
            self.contract_yaml_sources.append(YamlSource.from_str(yaml_str=contract_yaml_str))
        else:
            logger.error(
                f"...ignoring invalid contract_yaml_str '{contract_yaml_str}'.  "
                f"Expected string, but was {contract_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_file(self, soda_cloud_yaml_file_path: str) -> T:
        if isinstance(soda_cloud_yaml_file_path, str):
            if self.soda_cloud_yaml_source is None:
                logger.debug(f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'")
            else:
                logger.debug(
                    f"  ...with soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_file_path(yaml_file_path=soda_cloud_yaml_file_path)
        else:
            logger.error(
                f"...ignoring invalid soda_cloud_yaml_file_path '{soda_cloud_yaml_file_path}'. "
                f"Expected string, but was {soda_cloud_yaml_file_path.__class__.__name__}"
            )
        return self

    def with_soda_cloud_yaml_str(self, soda_cloud_yaml_str: str) -> T:
        if isinstance(soda_cloud_yaml_str, str):
            if self.soda_cloud_yaml_source is None:
                logger.debug(f"  ...with soda_cloud_yaml_str [{len(soda_cloud_yaml_str)}]")
            else:
                logger.debug(
                    f"  ...with soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                    f"Ignoring previously configured soda cloud '{self.soda_cloud_yaml_source}'"
                )
            self.soda_cloud_yaml_source = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        else:
            logger.error(
                f"...ignoring invalid soda_cloud_yaml_str '{soda_cloud_yaml_str}'. "
                f"Expected string, but was {soda_cloud_yaml_str.__class__.__name__}"
            )
        return self

    def with_soda_cloud(self, soda_cloud: object) -> T:
        logger.debug(f"  ...with provided soda_cloud '{soda_cloud}'")
        self.soda_cloud = soda_cloud
        return self

    def with_variable(self, key: str, value: str) -> T:
        if isinstance(key, str) and isinstance(value, str):
            logger.debug(f"  ...with variable '{key}'")
            self.variables[key] = value
        else:
            logger.error(
                f"Ignoring invalid variable '{key}'. "
                f"Expected key str and value string"
            )
        return self

    def with_variables(self, variables: dict[str, str]) -> T:
        if isinstance(variables, dict):
            logger.debug(f"  ...with variables {list(variables.keys())}")
            self.variables.update(variables)
        elif variables is None:
            if isinstance(self.variables, dict) and len(self.variables) > 1:
                logger.debug(f"  ...removing variables {list(self.variables.keys())} because variables set to None")
            self.variables = None
        else:
            logger.error(
                f"Ignoring invalid variables '{variables}'. "
                f"Expected dict, but was {variables.__class__.__name__}"
            )
        return self
