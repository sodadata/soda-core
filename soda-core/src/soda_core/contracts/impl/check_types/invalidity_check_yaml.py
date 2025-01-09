from __future__ import annotations

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import CheckYaml, ColumnYaml, CheckYamlParser, MissingAncValidityCheckYaml


class InvalidCheckYamlParser(CheckYamlParser):

    def get_check_type_names(self) -> list[str]:
        return ['invalid_count', 'invalid_percent']

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return InvalidCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class InvalidCheckYaml(MissingAncValidityCheckYaml):

    def __init__(self, check_yaml_object: YamlObject):
        super().__init__(check_yaml_object)
