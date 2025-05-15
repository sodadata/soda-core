from __future__ import annotations

import logging
from typing import Optional

from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
)

logger: logging.Logger = soda_logger


class FreshnessCheckYamlParser(CheckYamlParser):
    def get_check_type_names(self) -> list[str]:
        return ["freshness"]

    def parse_check_yaml(
        self, check_type_name: str, check_yaml_object: YamlObject, column_yaml: Optional[ColumnYaml]
    ) -> Optional[CheckYaml]:
        if column_yaml:
            logger.error(
                msg=f"The 'freshness' check type is not supported in column checks. Was on column '{column_yaml.name}'",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
        return FreshnessCheckYaml(type_name=check_type_name, check_yaml_object=check_yaml_object)


class FreshnessCheckYaml(MissingAncValidityCheckYaml):
    def __init__(self, type_name: str, check_yaml_object: YamlObject):
        super().__init__(type_name=type_name, check_yaml_object=check_yaml_object)
        self.column: str = check_yaml_object.read_string("column")
        self.now_variable: Optional[str] = check_yaml_object.read_string_opt("now_variable")
        self.unit: Optional[str] = check_yaml_object.read_string_opt("unit")
        if self.unit and not self.unit in ["minute", "hour", "day"]:
            logger.error(
                msg=f"Invalid freshness threshold unit: {self.unit}",
                extra={ExtraKeys.LOCATION: check_yaml_object.create_location_from_yaml_dict_key("unit")},
            )
            self.unit = None

    def get_valid_units(self) -> list[str]:
        return ["minute", "hour", "day"]
