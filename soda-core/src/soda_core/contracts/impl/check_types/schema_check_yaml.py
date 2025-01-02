from __future__ import annotations

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, CheckYamlParser


class SchemaCheckYamlParser(CheckYamlParser):

    def get_check_type_names(self) -> list[str]:
        return ['schema']

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return SchemaCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class SchemaCheckYaml(CheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject,
    ):
        super().__init__(
            check_yaml_object=check_yaml_object,
        )
