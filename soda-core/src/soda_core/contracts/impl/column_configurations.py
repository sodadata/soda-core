from __future__ import annotations

from dataclasses import dataclass
from numbers import Number


@dataclass
class MissingConfigurations:
    missing_values: list[str] | list[Number] | None
    missing_regex_sql: str | None


@dataclass
class ValidConfigurations:
    invalid_values: list[str] | list[Number] | None
    invalid_format: str | None
    invalid_regex_sql: str | None
    valid_values: list[str] | list[Number] | None
    valid_format: str | None
    valid_regex_sql: str | None
    valid_min: Number | None
    valid_max: Number | None
    valid_length: int | None
    valid_min_length: int | None
    valid_max_length: int | None
    valid_reference_data: ValidReferenceData | None

    def has_non_reference_data_configs(self) -> bool:
        return (
            self.invalid_values is not None
            or self.invalid_format is not None
            or self.invalid_regex_sql is not None
            or self.valid_values is not None
            or self.valid_format is not None
            or self.valid_regex_sql is not None
            or self.valid_min is not None
            or self.valid_max is not None
            or self.valid_length is not None
            or self.valid_min_length is not None
            or self.valid_max_length is not None
        )


@dataclass
class ValidReferenceData:
    dataset: str
    column: str
    columns: list[str]
