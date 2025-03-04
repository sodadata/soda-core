from __future__ import annotations

from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
)


class MissingCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["missing"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml], logs: Logs
    ) -> Optional[CheckYaml]:
        return MissingCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object, logs=logs)


class MissingCheckYaml(MissingAncValidityCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject, logs: Logs):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object, logs=logs)
