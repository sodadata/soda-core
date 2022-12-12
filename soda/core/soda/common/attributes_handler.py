from __future__ import annotations

from datetime import datetime

from soda.common.logs import Logs


class AttributeHandler:
    def __init__(self, logs: Logs) -> None:
        self.logs = logs

    def validate(self, attributes: dict[str, any], schema: list[dict]) -> tuple[dict[str, any]]:
        """Validates given attributes against given schema. Returns a dict with valid and a dict with invalid attributes."""
        valid = {}
        invalid = {}
        for k, v in attributes.items():
            is_valid = self.validate_attribute(k, v, schema)
            if is_valid:
                valid[k] = v
            else:
                invalid[k] = v

        return valid, invalid

    def validate_attribute(self, key: str, value: any, schema: list[dict]) -> bool:
        is_valid = False
        for schema_item in schema:
            if key == schema_item["name"]:
                schema_type = schema_item["type"]
                validity_method = f"is_valid_{schema_type.lower()}"

                try:
                    is_valid = getattr(self, validity_method)(value, schema_item)
                except AttributeError:
                    self.logs.error(f"Unsupported attribute type '{schema_type}'.")

        return is_valid

    def format_attribute(self, value: any):
        # Introduce formatting methods similar to validation methods if this gets more complex.
        if isinstance(value, datetime):
            return value.isoformat()

        return value

    @staticmethod
    def is_valid_checkbox(value: any, schema: dict[str, any]) -> bool:
        if value in schema["allowedValues"]:
            return True
        return False

    @staticmethod
    def is_valid_datetime(value: any, schema: dict[str, any]) -> bool:
        if isinstance(value, datetime):
            return True

        try:
            datetime.fromisoformat(value)
            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def is_valid_multiselect(value: any, schema: dict[str, any]) -> bool:
        if isinstance(value, list):
            for v in value:
                if v not in schema["allowedValues"]:
                    return False
            return True
        return False

    @staticmethod
    def is_valid_number(value: any, schema: dict[str, any]) -> bool:
        if isinstance(value, int) or isinstance(value, float):
            return True
        return False

    @staticmethod
    def is_valid_singleselect(value: any, schema: dict[str, any]) -> bool:
        if value in schema["allowedValues"]:
            return True
        return False

    @staticmethod
    def is_valid_text(value: any, schema: dict[str, any]) -> bool:
        if isinstance(value, str):
            return True
        return False
