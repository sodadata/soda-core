from __future__ import annotations

import logging
from textwrap import dedent
from typing import Optional

from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.yaml import YamlObject
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    ThresholdCheckYaml,
)

logger: logging.Logger = soda_logger


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
        self.expression: Optional[str] = check_yaml_object.read_string_opt("expression")
        if self.expression:
            self.expression = dedent(self.expression).strip()
        self.query: Optional[str] = check_yaml_object.read_string_opt("query")
        if self.query:
            self.query = dedent(self.query).strip()
        # Third form: a custom SQL query whose output keys are routed into the shared diagnostics-warehouse
        # fk_/fr_ tables (instead of a per-check frq_ table). The keys are hashed by Soda; see the
        # failed-rows extractor in soda-extensions.
        self.keys_query: Optional[str] = check_yaml_object.read_string_opt("keys_query")
        if self.keys_query:
            self.keys_query = dedent(self.keys_query).strip()
        self.rows_tested_query: Optional[str] = check_yaml_object.read_string_opt("rows_tested_query")
        if self.rows_tested_query:
            self.rows_tested_query = dedent(self.rows_tested_query).strip()
        # Maps each DWH key column name to the query output column that supplies it. Identity when omitted
        # (the query output column has the same name as the DWH key column). The set of DWH key columns is
        # not known at contract-parse time (it ships via the diagnostics-warehouse config), so completeness
        # of this mapping against the actual key columns is validated in soda-extensions, not here.
        self.key_column_mapping: Optional[dict[str, str]] = None
        key_column_mapping_object = check_yaml_object.read_object_opt("key_column_mapping")
        if key_column_mapping_object is not None:
            self.key_column_mapping = {}
            for mapping_key, mapping_value in key_column_mapping_object.yaml_dict.items():
                if not isinstance(mapping_value, str):
                    logger.error(
                        msg="In a 'failed_rows' check, 'key_column_mapping' must map each key column name "
                        "to a query output column name (a string)",
                        extra={ExtraKeys.LOCATION: key_column_mapping_object.location},
                    )
                else:
                    self.key_column_mapping[str(mapping_key)] = mapping_value

        forms_set: list[str] = [
            name
            for name, value in (("expression", self.expression), ("query", self.query), ("keys_query", self.keys_query))
            if value
        ]
        if len(forms_set) == 0:
            logger.error(
                msg="In a 'failed_rows' check, one of 'expression', 'query' or 'keys_query' is required",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
        elif len(forms_set) > 1:
            logger.error(
                msg=f"In a 'failed_rows' check, 'expression', 'query' and 'keys_query' are mutually exclusive, "
                f"but found: {', '.join(forms_set)}",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
        if self.metric == "percent" and (self.query or self.keys_query) and not self.rows_tested_query:
            logger.error(
                msg="In a 'failed_rows' check with metric 'percent' and 'query'/'keys_query', "
                "'rows_tested_query' is required",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
        if self.rows_tested_query and not (self.query or self.keys_query):
            logger.warning(
                msg="In a 'failed_rows' check, 'rows_tested_query' is only used with 'query' or 'keys_query' mode; "
                "expression mode already computes check_rows_tested automatically",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )
        if self.key_column_mapping is not None and not self.keys_query:
            logger.warning(
                msg="In a 'failed_rows' check, 'key_column_mapping' is only used with 'keys_query' mode",
                extra={ExtraKeys.LOCATION: check_yaml_object.location},
            )

    def get_valid_metrics(self) -> list[str]:
        return ["count", "percent"]
