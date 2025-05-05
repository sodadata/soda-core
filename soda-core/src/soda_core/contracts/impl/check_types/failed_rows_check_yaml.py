from __future__ import annotations

from textwrap import dedent
from typing import Optional

from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    ThresholdCheckYaml,
)


class FailedRowsCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["failed_rows"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return FailedRowsCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class FailedRowsCheckYaml(ThresholdCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.query: Optional[str] = check_yaml_object.read_string("query")
        if self.query:
            self.query = dedent(self.query).strip()

    def read_metric_count_percent(self, check_yaml_object: YamlObject) -> Optional[str]:
        return check_yaml_object.read_string_opt("metric")
