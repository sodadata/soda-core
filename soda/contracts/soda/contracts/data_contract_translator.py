from __future__ import annotations

import copy
import logging
from numbers import Number
from typing import Any, List

from soda.contracts.yaml import YamlParser, YamlList, YamlObject, YamlValue, YamlNumber

logger = logging.getLogger(__name__)


class DataContractTranslator:

    def __init__(self):
        self.yaml_parser = YamlParser()

    def translate_data_contract_yaml_str(self, data_contract_yaml_str: str) -> str:
        """
        Parses a data contract YAML string into a SodaCL YAML str that can be fed into
        SodaCLParser.parse_sodacl_yaml_str
        """
        data_contract_yaml_object: YamlValue = self.yaml_parser.parse_yaml_str(data_contract_yaml_str)
        ruamel_sodacl_yaml_dict = self._create_sodacl_yaml_dict(data_contract_yaml_object)
        return self.yaml_parser.write_yaml_str(ruamel_sodacl_yaml_dict)

    def _create_sodacl_yaml_dict(self, data_contract_yaml_object: YamlObject) -> dict:
        dataset_name_str: str | None = data_contract_yaml_object.read_string("dataset")

        sodacl_checks: list[Any] = []

        contract_columns: YamlObject | None = data_contract_yaml_object.read_yaml_list("columns")
        if contract_columns:
            columns = {}
            sodacl_checks.append({"schema": {"fail": {"when mismatching columns": columns}}})

            for contract_column in contract_columns:
                contract_column_yaml_object: YamlObject = contract_column
                column_name: str = contract_column_yaml_object.read_string("name")

                data_type: str | None = contract_column_yaml_object.read_string_opt("data_type")
                columns[column_name] = data_type

                sodacl_missing_config = {
                    k.replace("_", " "): v.unpacked()
                    for k, v in contract_column_yaml_object.items()
                    if k.startswith("missing_")
                }
                if contract_column_yaml_object.read_bool_opt("not_null") or sodacl_missing_config:
                    if sodacl_missing_config:
                        sodacl_checks.append({f"missing_count({column_name}) = 0": sodacl_missing_config})
                    else:
                        sodacl_checks.append(f"missing_count({column_name}) = 0")

                sodacl_validity_config = {
                    k.replace("_", " "): v.unpacked()
                    for k, v in contract_column_yaml_object.items()
                    if k.startswith("valid_") or k.startswith("invalid_")
                }
                if sodacl_validity_config:
                    if sodacl_missing_config:
                        combined_configs = copy.deepcopy(sodacl_missing_config)
                        combined_configs.update(sodacl_validity_config)
                        sodacl_validity_config = combined_configs
                    sodacl_checks.append({f"invalid_count({column_name}) = 0": sodacl_validity_config})

                if contract_column_yaml_object.read_bool_opt("unique"):
                    sodacl_checks.append(f"duplicate_count({column_name}) = 0")

                column_checks: YamlList = contract_column_yaml_object.read_yaml_list_opt("checks")

                if column_checks:
                    for column_check in column_checks:
                        sodacl_column_check = self._parse_check(check=column_check, column_name=column_name)
                        if sodacl_column_check:
                            sodacl_checks.append(sodacl_column_check)
                        else:
                            logging.error(f"Could not build sodacl check for {column_check.unpacked()}")

        checks: YamlList | None = data_contract_yaml_object.read_yaml_list_opt("checks")
        if checks:
            for check in checks:
                column_name: str = check.read_string_opt("column")
                sodacl_check = self._parse_check(check=check, column_name=column_name)
                if sodacl_check:
                    sodacl_checks.append(sodacl_check)

        sodacl_dict: dict = {f"checks for {dataset_name_str}": sodacl_checks}

        return sodacl_dict

    def _parse_check(self, check: YamlObject, column_name: str | None = None) -> object | None:
        check_type = check.read_string("type")

        if not check_type:
            logger.error(f"Check must have 'type' defined. {check.location}")
            return None

        if check_type == "reference":
            ref_dataset: str | None = check.read_string("dataset")
            ref_column: str | None = check.read_string("column")
            if not ref_dataset or not ref_column:
                logger.error(f"Reference check must have 'dataset' and 'column'. {check.location}")
                return None

            sample_limit: Number | None = check.read_number_opt("samples_limit")
            reference_check_line = (
                f"values in ({column_name}) must exist in {ref_dataset} ({ref_column})"
            )
            if sample_limit:
                return {reference_check_line: {"samples limit": sample_limit}}
            else:
                return reference_check_line

        metric = f"{check_type}({column_name})" if column_name else check_type
        fail_configs = {
            k: v for k, v in check.items()
            if k.startswith("fail_")
        }
        warn_configs = {
            k: v for k, v in check.items()
            if k.startswith("warn_")
        }
        check_configs = {
            k: v for k, v in check.unpacked().items()
            if k not in ["type", "column"] and not k.startswith("warn_") and not k.startswith("fail_")
        }

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

    def _is_short_style_not_null_check(self, check: YamlObject) -> bool:
        return len(check) == 1 and check.read_bool_opt("not_null") is True

    def _is_missing_config_check(self, check: YamlObject) -> bool:
        return all(k.startswith("missing_") for k in check)

    def _is_invalid_config_check(self, check: YamlObject) -> bool:
        return all(k.startswith("valid_") or k.startswith("invalid_") for k in check)

    def _is_short_style_unique_check(self, check: YamlObject) -> bool:
        return len(check) == 1 and check.get("unique") is True

    def _create_check(self, check_line: str, check_configs: dict) -> object:
        return {check_line: check_configs} if check_configs else check_line

    def _parse_threshold_list_of_2_numbers(self, threshold_value: YamlValue) -> List[Number] | None:
        threshold_value_unpacked = threshold_value.unpacked()
        if (isinstance(threshold_value_unpacked, List)
                and len(threshold_value_unpacked) == 2
                and all(isinstance(v, Number) for v in threshold_value_unpacked)):
            return threshold_value_unpacked
        return None
