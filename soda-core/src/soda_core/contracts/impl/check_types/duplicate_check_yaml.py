from __future__ import annotations

import logging
from typing import Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
    ThresholdCheckYaml,
)

logger: logging.Logger = soda_logger


class DuplicateCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["duplicate"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        if column_yaml:
            return ColumnDuplicateCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)
        else:
            return MultiColumnDuplicateCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class ColumnDuplicateCheckYaml(MissingAncValidityCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)


class MultiColumnDuplicateCheckYaml(ThresholdCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.columns: Optional[list[str]] = check_yaml_object.read_list_of_strings("columns")
        if self.columns:
            if len(self.columns) == 0:
                logger.error("'columns' must be a list of column names, but was empty")

    def get_valid_metrics(self) -> list[str]:
        return ["count", "percent"]
