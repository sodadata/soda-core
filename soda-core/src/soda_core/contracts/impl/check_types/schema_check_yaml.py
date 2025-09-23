from __future__ import annotations

from typing import Optional

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
)


class SchemaCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["schema"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return SchemaCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class SchemaCheckYaml(CheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.allow_extra_columns: Optional[bool] = (
            check_yaml_object.read_bool_opt("allow_extra_columns") if check_yaml_object else None
        )
        self.allow_other_column_order: Optional[bool] = (
            check_yaml_object.read_bool_opt("allow_other_column_order") if check_yaml_object else None
        )
