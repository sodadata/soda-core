from __future__ import annotations

from soda.contract.parser.data_contract_yaml import YamlString, YamlObject


class Check:
    def __init__(self, check_yaml: YamlString | YamlObject):
        self.check_yaml: YamlString | YamlObject = check_yaml

    def is_schema(self) -> bool:
        return isinstance(self.check_yaml, YamlString) and self.check_yaml.value == "schema"
