from __future__ import annotations

<<<<<<< HEAD
from soda.contract.parser.parser_yaml import YamlObject, YamlString

=======
from soda.contract.parser.data_contract_yaml import YamlObject, YamlString

>>>>>>> 54dd0f18 (Contracts cleanup)


class Check:
    def __init__(self, check_yaml: YamlString | YamlObject):
        self.check_yaml: YamlString | YamlObject = check_yaml

    def is_schema(self) -> bool:
        return isinstance(self.check_yaml, YamlString) and self.check_yaml.value == "schema"
