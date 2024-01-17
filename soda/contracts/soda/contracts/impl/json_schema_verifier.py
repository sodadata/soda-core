from __future__ import annotations

import json

from jsonschema.validators import Draft7Validator

from soda.contracts.impl.logs import Logs


class JsonSchemaVerifier:

    def __init__(self, logs: Logs | None = None):
        self.logs: Logs = logs if logs else Logs()

    def verify(self, yaml_object: object) -> None:
        """
        Verifies that the YAML data structure matches the data contract schema.
        Swallows all errors and exceptions and appends them to self.logs.
        """
        contracts_dir = __file__[:-len("/impl/json_schema_verifier.py")]
        contract_schema_json_file_path = f"{contracts_dir}/soda_data_contract_json_schema_1_0_0.json"
        with open(contract_schema_json_file_path) as f:
            contract_schema_json_str = f.read()
            schema_dict = json.loads(contract_schema_json_str)
            validator = Draft7Validator(schema_dict)
            for error in validator.iter_errors(instance=yaml_object):
                error_path_text = "contract document level" if len(error.path) == 0 else "/".join(error.path)
                self.logs.error(f"{error.message} ({error_path_text})")
