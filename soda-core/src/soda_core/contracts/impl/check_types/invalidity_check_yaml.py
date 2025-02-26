from __future__ import annotations

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, CheckYamlParser, MissingAncValidityCheckYaml


class InvalidCheckYamlParser(CheckYamlParser):

    def get_check_type_names(self) -> list[str]:
        return ['invalid']

    def parse_check_yaml(
        self,
        check_type_name: str,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
        logs: Logs
    ) -> CheckYaml | None:
        return InvalidCheckYaml(
            type_name=check_type_name,
            check_yaml_object=check_yaml_object,
            logs=logs
        )


class InvalidCheckYaml(MissingAncValidityCheckYaml):

    def __init__(self, type_name: str, check_yaml_object: YamlObject, logs: Logs):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object, logs=logs)
