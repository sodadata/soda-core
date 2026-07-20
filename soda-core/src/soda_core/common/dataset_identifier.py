from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from soda_core.common.sql_dialect import SqlDialect
    from soda_core.common.statements.table_types import FullyQualifiedObjectName

from soda_core.common.consistent_hash_builder import ConsistentHashBuilder
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


class DatasetIdentifier:
    def __init__(self, data_source_name: str, prefixes: list[str], dataset_name: str):
        self.data_source_name = data_source_name
        self.prefixes = prefixes
        self.dataset_name = dataset_name

    @classmethod
    def parse(cls, dataset_qualified_name: Optional[str]) -> DatasetIdentifier:
        from soda_core.common.exceptions import InvalidDatasetQualifiedNameException

        if not dataset_qualified_name:
            raise InvalidDatasetQualifiedNameException("Dataset DQN must be a valid string and cannot be None")

        parts = dataset_qualified_name.split("/")
        if len(parts) < 2:
            raise InvalidDatasetQualifiedNameException("Dataset DQN must contain at least a data source and a dataset")

        data_source_name = parts[0]
        dataset_name = parts[-1]
        prefixes = parts[1:-1] if len(parts) > 2 else []

        return cls(data_source_name, prefixes, dataset_name)

    @classmethod
    def from_object(
        cls,
        data_source_name: str,
        sql_dialect: "SqlDialect",
        fully_qualified_object_name: "FullyQualifiedObjectName",
    ) -> "DatasetIdentifier":
        """Build a dialect-correct DQN from a discovered FullyQualifiedObjectName.

        A prefix component is included only when the dialect has that tier
        (prefix-index hook not None) and the object carries a value;
        database precedes schema, as in extract_database_from_prefix.
        """
        prefixes: list[str] = []
        if (
            sql_dialect.get_database_prefix_index() is not None
            and fully_qualified_object_name.database_name is not None
        ):
            prefixes.append(fully_qualified_object_name.database_name)
        if sql_dialect.get_schema_prefix_index() is not None and fully_qualified_object_name.schema_name is not None:
            prefixes.append(fully_qualified_object_name.schema_name)
        return cls(
            data_source_name=data_source_name,
            prefixes=prefixes,
            dataset_name=fully_qualified_object_name.get_object_name(),
        )

    def to_string(self) -> str:
        return "/".join([self.data_source_name] + self.prefixes + [self.dataset_name])

    def to_hash(self) -> str:
        consistent_hash_builder: ConsistentHashBuilder = ConsistentHashBuilder(hash_string_length=32)  # Use 32 chars
        consistent_hash_builder.add(self.to_string())
        return consistent_hash_builder.get_hash()

    def __repr__(self):
        return (
            f"DatasetIdentifier(data_source='{self.data_source_name}', "
            f"prefixes={self.prefixes}, dataset='{self.dataset_name}')"
        )
