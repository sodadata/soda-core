from __future__ import annotations

from abc import ABC, abstractmethod

from soda.common.logs import Logs
from soda.common.yaml import YamlObject
from soda.contracts.impl.contract_yaml import CheckYaml


class CheckType(ABC):

    @abstractmethod
    def get_check_type_name(self) -> str:
        pass

    @abstractmethod
    def parse_check_yaml(self, check_yaml_object: YamlObject, logs: Logs) -> CheckYaml | None:
        pass
