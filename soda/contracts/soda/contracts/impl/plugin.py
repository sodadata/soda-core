from __future__ import annotations

import importlib
from abc import ABC, abstractmethod

from soda.contracts.contract import ContractResult
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile


class Plugin(ABC):

    @classmethod
    def create(cls, plugin_name: str, plugin_yaml_files: list[YamlFile], logs: Logs) -> Plugin | None:
        # dynamically load plugin based on name and feed plugin_yaml_files in the constructor
        try:
            module = importlib.import_module(f"soda.{plugin_name}.{plugin_name}_plugin")
            plugin_class_name = cls.get_plugin_class_name(plugin_name)
            class_ = getattr(module, plugin_class_name)
            return class_(logs, plugin_name, plugin_yaml_files)
        except ModuleNotFoundError as e:
            logs.error(f"Could not instantiate plugin {plugin_name}: {e}")
            return None

    def __init__(self, logs: Logs, plugin_name: str, plugin_yaml_files: list[YamlFile]):
        self.logs: Logs = logs
        self.plugin_name = plugin_name
        self.plugin_yaml_files: list[YamlFile] = plugin_yaml_files

    @classmethod
    def get_plugin_class_name(cls, plugin_name: str) -> str:
        # if "integration" == plugin_name:
        #     return "IntegrationPlugin"
        return f"{plugin_name[0:1].upper()}{plugin_name[1:]}Plugin"

    @abstractmethod
    def process_contract_results(self, contract_result: ContractResult) -> None:
        pass
