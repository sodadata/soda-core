from __future__ import annotations

import copy
import logging
from numbers import Number
from typing import Any, List, Dict

from soda.contracts.impl.yaml import YamlParser, YamlList, YamlObject, YamlValue, YamlNumber, YamlWriter
from soda.contracts.impl.logs import Logs

logger = logging.getLogger(__name__)


class ContractTranslator:

    def __init__(self, logs: Logs | None = None):
        super().__init__()
        self.logs = logs if logs else Logs()
        self.missing_value_configs_by_column = {}
        self.skip_schema_validation: bool = False

    def translate_data_contract(self, data_contract_yaml_object: YamlObject) -> dict:
        dataset_name_str: str | None = data_contract_yaml_object.read_string("dataset")
        schema_name_str: str | None = data_contract_yaml_object.read_string_opt("schema")

        sodacl_checks: list[Any] = []

        contract_columns: YamlObject | None = data_contract_yaml_object.read_yaml_list("columns")
        if contract_columns:
            columns: Dict[str,str | None] = {}
            optional_columns: List[str] = []
            schema_fail_dict = {"when mismatching columns": columns}
            sodacl_checks.append({"schema": {"fail": schema_fail_dict}})

            for contract_column in contract_columns:
                contract_column_yaml_object: YamlObject = contract_column
                column_name: str = contract_column_yaml_object.read_string("name")

                data_type: str | None = contract_column_yaml_object.read_string_opt("data_type")
                columns[column_name] = data_type

                if contract_column_yaml_object.read_bool_opt("optional", default_value=False):
                    optional_columns.append(column_name)

                column_checks: YamlList = contract_column_yaml_object.read_yaml_list_opt("checks")

                if column_checks:
                    for column_check in column_checks:
                        sodacl_column_check = self._parse_column_check(check=column_check, column_name=column_name)
                        if sodacl_column_check:
                            sodacl_checks.append(sodacl_column_check)
                        else:
                            logging.error(f"Could not build sodacl check for {column_check.unpacked()}")

            if optional_columns:
                schema_fail_dict["with optional columns"] = optional_columns

        checks: YamlList | None = data_contract_yaml_object.read_yaml_list_opt("checks")
        if checks:
            for check in checks:
                column_name: str = check.read_string_opt("column")
                sodacl_check = self._parse_dataset_check(check=check, column_name=column_name)
                if sodacl_check:
                    sodacl_checks.append(sodacl_check)

        sodacl_dict: dict = {f"checks for {dataset_name_str}": sodacl_checks}

        return sodacl_dict

    def _parse_column_check(self, check: YamlObject, column_name: str) -> object | None:
        check_type = check.read_string("type")

        if check_type in ["not_null", "missing", "missing_count", "missing_percent"]:
            return self._parse_column_missing_check(check=check, check_type=check_type, column_name=column_name)

        elif check_type in ["invalid", "invalid_count", "invalid_percent"]:
            return self._parse_column_invalid_check(check=check, check_type=check_type, column_name=column_name)

        elif check_type in ["unique", "duplicate_count"]:
            return self._parse_column_duplicates_check(check=check, check_type=check_type, column_name=column_name)

        else:
            return self._parse_column_aggregation_check(check=check, check_type=check_type, column_name=column_name)

    def _parse_column_missing_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
        check_configs: dict = self._parse_check_configs(check)
        missing_configs: dict = {k: v for k, v in check_configs.items() if k.startswith("missing")}
        self.missing_value_configs_by_column[column_name] = missing_configs

        if check_type in ["missing", "not_null"]:
            return self._create_check(f"missing_count({column_name}) = 0", check_configs)

        metric = f"{check_type}({column_name})"
        return self._create_metric_with_threshold(metric, check)

    def _parse_column_invalid_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
        check_configs: dict = self._parse_check_configs(check)

        missing_configs: dict | None = self.missing_value_configs_by_column.get(column_name)
        if missing_configs:
            new_check_configs = copy.deepcopy(missing_configs)
            new_check_configs.update(check_configs)
            check_configs = new_check_configs

        valid_values_column: YamlObject = check.read_yaml_object_opt("valid_values_column")
        if valid_values_column is not None:
            if not isinstance(valid_values_column, YamlObject):
                self.error(
                    f"Expected object for contents of 'valid_values_column', "
                    f"but was {type(valid_values_column)}, {check.location}"
                )
            else:
                ref_dataset: str | None = valid_values_column.read_string("dataset")
                ref_column: str | None = valid_values_column.read_string("column")
                if not ref_dataset or not ref_column:
                    logger.error(f"Reference check must have 'dataset' and 'column'. {check.location}")
                    return None
                reference_check_line = (
                    f"values in ({column_name}) must exist in {ref_dataset} ({ref_column})"
                )
                check_configs.pop("valid values column")
                return self._create_check(reference_check_line, check_configs)

        if check_type == "invalid":
            return self._create_check(f"invalid_count({column_name}) = 0", check_configs)

        metric = f"{check_type}({column_name})"
        return self._create_metric_with_threshold(metric, check)

    def _parse_column_duplicates_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
        if check_type == "unique":
            check_configs: dict = self._parse_check_configs(check)
            return self._create_check(f"duplicate_count({column_name}) = 0", check_configs)
        metric = f"duplicate_count({column_name})"
        return self._create_metric_with_threshold(metric, check)

    def _parse_column_aggregation_check(self, check: YamlObject, check_type: str, column_name: str) -> object | None:
        metric = f"{check_type}({column_name})"
        return self._create_metric_with_threshold(metric, check)

    def _parse_dataset_check(self, check: YamlObject, column_name: str) -> object | None:
        check_type = check.read_string("type")

        if check_type == "row_count":
            return self._create_metric_with_threshold("row_count", check)

        self.error(f"Unsupported dataset check type {check_type}: {check.location}")

    def _create_metric_with_threshold(self, metric: str, check: YamlObject) -> object | None:
        fail_configs = self._parse_fail_configs(check)
        warn_configs = self._parse_warn_configs(check)
        check_configs = self._parse_check_configs(check)
        if warn_configs:
            logger.error(f"Warnings not yet supported: {check.location}")

        elif len(fail_configs) > 1:
            logger.error(f"Combination of multiple fail thresholds is not yet supported: {check.location}")

        elif len(fail_configs) == 1:
            fail_config, threshold_value = next(iter(fail_configs.items()))
            fail_single_thresholds = {
                "fail_when_greater_than": "<=",
                "fail_when_greater_than_or_equal": "<",
                "fail_when_less_than": ">=",
                "fail_when_less_than_or_equal": ">",
                "fail_when_is": "!=",
                "fail_when_not": "="
            }
            fail_range_thresholds = {
                "fail_when_not_between": "",
                "fail_when_between": "not "
            }
            if fail_config in fail_single_thresholds:
                if isinstance(threshold_value, YamlNumber):
                    return self._create_check(
                        check_line=f"{metric} {fail_single_thresholds[fail_config]} {threshold_value.unpacked()}",
                        check_configs=check_configs
                    )
                else:
                    logging.error(f"Invalid threshold value {threshold_value}. Expected number. {check.location}")
                    return None
            elif fail_config in fail_range_thresholds:
                threshold_list = self._parse_threshold_list_of_2_numbers(threshold_value)
                if threshold_list:
                    conditional_not = fail_range_thresholds[fail_config]
                    return self._create_check(
                        check_line=f"{metric} {conditional_not}between {threshold_list[0]} and {threshold_list[1]}",
                        check_configs=check_configs
                    )
                logging.error(
                    f"Invalid threshold value {threshold_value}. Expected list of 2 numbers.  {check.location}")
                return None
            logging.error(
                f"TODO figure out which parsing error is here.  No threshold specified? {check.location}")
            return None

    def _parse_warn_configs(self, check):
        warn_configs = {
            k: v for k, v in check.items()
            if k.startswith("warn_")
        }
        return warn_configs

    def _parse_fail_configs(self, check):
        fail_configs = {
            k: v for k, v in check.items()
            if k.startswith("fail_")
        }
        return fail_configs

    def _parse_check_configs(self, check):
        check_configs = {
            k.replace("_", " "): v for k, v in check.unpacked().items()
            if k not in ["type"] and not k.startswith("warn_") and not k.startswith("fail_")
        }
        return check_configs

    def _create_check(self, check_line: str, check_configs: dict) -> object:
        return {check_line: check_configs} if check_configs else check_line

    def _parse_threshold_list_of_2_numbers(self, threshold_value: YamlValue) -> List[Number] | None:
        threshold_value_unpacked = threshold_value.unpacked()
        if (isinstance(threshold_value_unpacked, List)
                and len(threshold_value_unpacked) == 2
                and all(isinstance(v, Number) for v in threshold_value_unpacked)):
            return threshold_value_unpacked
        return None

    def has_errors(self) -> bool:
        return self.logs.has_errors()
