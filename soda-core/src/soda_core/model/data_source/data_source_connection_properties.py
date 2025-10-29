from abc import ABC
from typing import ClassVar, Dict

from pydantic import BaseModel, ConfigDict, SecretStr


class DataSourceConnectionProperties(BaseModel, ABC):
    model_config = ConfigDict(frozen=True, extra="allow", validate_by_name=True)

    field_mapping: ClassVar[Dict[str, str]] = {}

    def to_connection_kwargs(self) -> dict:
        data = self.model_dump()  # Pydantic v2
        model_fields = set(self.__class__.model_fields.keys())

        # Known fields (declared ones)
        known_fields = {field_name: value for field_name, value in data.items() if field_name in model_fields}

        # Unknown fields (dynamic extras)
        unknown_fields = {field_name: value for field_name, value in data.items() if field_name not in model_fields}

        # Apply field renaming if mapping exists
        mapped_known_fields = {}
        for k, v in known_fields.items():
            output_key = self.field_mapping.get(k, k)  # Rename if mapped, otherwise same name
            if v is not None:
                mapped_known_fields[output_key] = v

        # Merge known and unknown fields
        all_kwargs = {**mapped_known_fields, **unknown_fields}

        # Final cleaning: unwrap secrets + drop None
        cleaned_kwargs = {}
        for k, v in all_kwargs.items():
            if isinstance(v, SecretStr):
                v = v.get_secret_value()
            if v is not None:
                cleaned_kwargs[k] = v

        return cleaned_kwargs
