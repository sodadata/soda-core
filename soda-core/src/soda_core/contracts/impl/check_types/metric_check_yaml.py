from __future__ import annotations

import logging

from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.sql_dialect import *
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
)

logger: logging.Logger = soda_logger


class MetricCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["metric"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        return MetricCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class MetricCheckYaml(MissingAncValidityCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.expression: Optional[str] = check_yaml_object.read_string_opt("expression")
        if self.expression:
            self.expression = dedent(self.expression).strip()
        self.query: Optional[str] = check_yaml_object.read_string_opt("query")
        if self.query:
            self.query = dedent(self.query).strip()
        if not self.expression and not self.query:
            logger.error(
                msg="In a 'metric' check, either 'expression' or 'query' is required",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
