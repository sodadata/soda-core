from __future__ import annotations

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_verification_impl import CheckType
from soda_core.contracts.impl.contract_yaml import CheckYaml, ContractYaml, ColumnYaml


class MissingCheck(CheckYaml):

    def __init__(self, check_yaml_object: YamlObject):
        super().__init__()
        self.check_yaml_object = check_yaml_object


class MissingCheckType(CheckType):

    def get_check_type_name(self) -> str:
        return "missing"

    def parse_check_yaml(self, check_yaml_object: YamlObject, logs: Logs) -> CheckYaml | None:
        return MissingCheck(check_yaml_object=check_yaml_object)
