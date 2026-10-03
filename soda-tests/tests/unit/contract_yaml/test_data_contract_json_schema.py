import json
from importlib import resources

SCHEMA_RESOURCE = "soda_data_contract_json_schema_1_0_0.json"


def _load_schema() -> dict:
    schema_text = resources.files("soda_core.contracts").joinpath(SCHEMA_RESOURCE).read_text(encoding="utf-8")
    return json.loads(schema_text)


def test_dataset_level_check_attributes_reuses_attributes_definition():
    schema = _load_schema()

    assert schema["additionalProperties"] is False
    assert "check_attributes" in schema["properties"]
    assert schema["properties"]["check_attributes"]["$ref"] == "#/$defs/attributes"
