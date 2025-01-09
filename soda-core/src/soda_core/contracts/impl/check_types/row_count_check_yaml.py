from __future__ import annotations

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import CheckYamlParser, ColumnYaml, CheckYaml, ThresholdCheckYaml


class RowCountCheckYamlParser(CheckYamlParser):

    def get_check_type_names(self) -> list[str]:
        return ['row_count']

    def parse_check_yaml(
        self,
        check_yaml_object: YamlObject,
        column_yaml: ColumnYaml | None,
    ) -> CheckYaml | None:
        return RowCountCheckYaml(
            check_yaml_object=check_yaml_object,
        )


class RowCountCheckYaml(ThresholdCheckYaml):

    def __init__(
        self,
        check_yaml_object: YamlObject,
    ):
        super().__init__(
            check_yaml_object=check_yaml_object
        )
