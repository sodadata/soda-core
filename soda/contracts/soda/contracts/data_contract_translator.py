from __future__ import annotations

from typing import Any

from contracts.yaml import Yaml, YamlList, YamlObject, YamlValue


class DataContractTranslator:
    def __init__(self):
        pass

    def translate_data_contract_yaml_str(self, data_contract_yaml_str: str) -> str:
        """
        Parses a data contract YAML string into a SodaCL YAML str that can be fed into
        SodaCLParser.parse_sodacl_yaml_str
        """

        data_contract_yaml_object: YamlValue = Yaml.parse_yaml_str(data_contract_yaml_str)

        ruamel_sodacl_yaml_dict = self._create_sodacl_yaml_dict(data_contract_yaml_object)

        return Yaml.write_yaml_str(ruamel_sodacl_yaml_dict)

    def _create_sodacl_yaml_dict(self, data_contract_yaml_object: YamlObject) -> dict:
        dataset_name_str: str | None = data_contract_yaml_object.read_string("dataset")

        sodacl_checks: list[Any] = []

        schema: YamlObject | None = data_contract_yaml_object.read_yaml_list("columns")
        if schema:
            columns = {}
            sodacl_checks.append({"schema": {"fail": {"when mismatching columns": columns}}})

            for column_schema_details_object in schema:
                column_schema_details: YamlObject = column_schema_details_object
                column_name: str = column_schema_details.read_string("name")

                data_type = None
                if column_schema_details:
                    data_type = column_schema_details.read_string_opt("data_type")

                    if column_schema_details.read_bool_opt("not_null"):
                        sodacl_checks.append(f"missing_count({column_name}) = 0")

                    if column_schema_details.read_bool_opt("unique"):
                        sodacl_checks.append(f"duplicate_count({column_name}) = 0")

                    valid_format: str | None = column_schema_details.read_string_opt("valid_format")
                    if valid_format:
                        sodacl_checks.append({f"invalid_count({column_name}) = 0": {"valid format": valid_format}})

                columns[column_name] = data_type

        checks: YamlList | None = data_contract_yaml_object.read_yaml_list_opt("checks")
        if checks:
            sodacl_checks.extend(checks.unpacked())

        sodacl_dict: dict = {f"checks for {dataset_name_str}": sodacl_checks}

        return sodacl_dict
