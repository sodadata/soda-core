from __future__ import annotations

import logging
from typing import Optional

from soda_core.common.exceptions import InvalidDatasetQualifiedNameException
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.yaml import YamlObject

logger: logging.Logger = soda_logger


class DatasetIdentifier:
    def __init__(self, data_source_name: str, prefixes: list[str], dataset_name: str):
        self.data_source_name = data_source_name
        self.prefixes = prefixes
        self.dataset_name = dataset_name

    @classmethod
    def read(cls, yaml_object: YamlObject, yaml_key: str, required: bool = True) -> Optional[str]:
        if yaml_object is None:
            return None
        dataset_qualified_name: Optional[str] = yaml_object.read_string_opt(yaml_key)
        if isinstance(dataset_qualified_name, str):
            parts = dataset_qualified_name.split("/")
            if len(parts) < 2:
                logger.error(
                    msg=f"Invalid dataset qualified name in '{yaml_key}': '{dataset_qualified_name}' "
                    f"must be slash-separated, fully qualified dataset name.",
                    extra={ExtraKeys.LOCATION: yaml_object.create_location_from_yaml_dict_key(yaml_key)},
                )
                return None
        elif required:
            logger.error(msg=f"'{yaml_key}' is required", extra={ExtraKeys.LOCATION: yaml_object.location})
        return dataset_qualified_name

    @classmethod
    def parse(cls, dataset_qualified_name: Optional[str]) -> DatasetIdentifier:
        if not dataset_qualified_name:
            raise InvalidDatasetQualifiedNameException("Identifier must be a valid string and cannot be None")

        parts = dataset_qualified_name.split("/")
        if len(parts) < 2:
            raise InvalidDatasetQualifiedNameException("Identifier must contain at least a data source and a dataset")

        data_source_name = parts[0]
        dataset_name = parts[-1]
        prefixes = parts[1:-1] if len(parts) > 2 else []

        return cls(data_source_name, prefixes, dataset_name)

    def to_string(self) -> str:
        return "/".join([self.data_source_name] + self.prefixes + [self.dataset_name])

    def __repr__(self):
        return (
            f"DatasetIdentifier(data_source='{self.data_source_name}', "
            f"prefixes={self.prefixes}, dataset='{self.dataset_name}')"
        )
