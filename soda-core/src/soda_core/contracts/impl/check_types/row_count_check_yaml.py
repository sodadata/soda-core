from __future__ import annotations

from typing import Optional

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    ThresholdCheckYaml,
)


class RowCountCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["row_count"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return RowCountCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class RowCountCheckYaml(ThresholdCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
