from __future__ import annotations

import logging
from typing import Optional

from soda_core.check_collections.impl.check_collection_yaml import (
    CheckCollectionYaml,
    CheckCollectionYamlExtension,
    CheckYaml,
    CheckYamlParser,
    ColumnYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RangeYaml,
    RegexFormat,
    ThresholdCheckYaml,
    ThresholdYaml,
    ValidReferenceDataYaml,
    VariableYaml,
)
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource, YamlObject

logger: logging.Logger = soda_logger


class ContractYaml(CheckCollectionYaml):
    _DISPLAY_NAME = "contract"

    @staticmethod
    def _resolve_yaml_source(
        contract_yaml_source: Optional[ContractYamlSource],
        check_collection_yaml_source: Optional[ContractYamlSource],
    ) -> Optional[ContractYamlSource]:
        """Normalize the legacy and current source kwargs to a single value.

        ``contract_yaml_source`` is the historical public-API name and remains
        supported for backwards compatibility; ``check_collection_yaml_source``
        is the parent-class name used by the abstract base. Exactly one (or
        neither) must be passed.
        """
        if contract_yaml_source is not None and check_collection_yaml_source is not None:
            raise TypeError("Pass either contract_yaml_source (legacy) or check_collection_yaml_source, not both")
        return contract_yaml_source if contract_yaml_source is not None else check_collection_yaml_source

    @classmethod
    def parse(
        cls,
        contract_yaml_source: Optional[ContractYamlSource] = None,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
        check_collection_yaml_source: Optional[ContractYamlSource] = None,
    ) -> "ContractYaml":
        return super().parse(
            check_collection_yaml_source=cls._resolve_yaml_source(contract_yaml_source, check_collection_yaml_source),
            provided_variable_values=provided_variable_values,
            data_timestamp=data_timestamp,
            primary_data_source_impl=primary_data_source_impl,
        )

    def __init__(
        self,
        contract_yaml_source: Optional[ContractYamlSource] = None,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
        check_collection_yaml_source: Optional[ContractYamlSource] = None,
    ):
        super().__init__(
            check_collection_yaml_source=self._resolve_yaml_source(contract_yaml_source, check_collection_yaml_source),
            provided_variable_values=provided_variable_values,
            data_timestamp=data_timestamp,
            primary_data_source_impl=primary_data_source_impl,
        )

    @property
    def contract_yaml_source(self) -> ContractYamlSource:
        """Backwards-compatible alias for self.check_collection_yaml_source."""
        return self.check_collection_yaml_source

    @property
    def contract_yaml_object(self) -> YamlObject:
        """Backwards-compatible alias for self.check_collection_yaml_object."""
        return self.check_collection_yaml_object


# Re-exports for backwards compatibility
__all__ = [
    "ContractYaml",
    "CheckCollectionYaml",
    "CheckCollectionYamlExtension",
    "CheckYaml",
    "CheckYamlParser",
    "ColumnYaml",
    "MissingAncValidityCheckYaml",
    "MissingAndValidityYaml",
    "RangeYaml",
    "RegexFormat",
    "ThresholdCheckYaml",
    "ThresholdYaml",
    "ValidReferenceDataYaml",
    "VariableYaml",
]
