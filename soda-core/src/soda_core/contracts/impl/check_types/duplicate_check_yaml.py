from __future__ import annotations

from typing import Optional

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
)


class DuplicateCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["duplicate"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return DuplicateCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class DuplicateCheckYaml(MissingAncValidityCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
