from typing import Optional

from soda_core.common.exceptions import InvalidDatasetQualifiedNameException


class DatasetIdentifier:
    def __init__(self, data_source_name: str, prefixes: list[str], dataset_name: str):
        self.data_source_name = data_source_name
        self.prefixes = prefixes
        self.dataset_name = dataset_name

    @classmethod
    def parse(cls, identifier: Optional[str]) -> "DatasetIdentifier":
        if not identifier:
            raise InvalidDatasetQualifiedNameException("Identifier must be a valid string and cannot be None")

        parts = identifier.split("/")
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
