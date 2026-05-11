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
from soda_core.common.yaml import ContractYamlSource

logger: logging.Logger = soda_logger


class ContractYaml(CheckCollectionYaml):
    @classmethod
    def parse(
        cls,
        contract_yaml_source: Optional[ContractYamlSource] = None,
        provided_variable_values: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        primary_data_source_impl: Optional[DataSourceImpl] = None,
        check_collection_yaml_source: Optional[ContractYamlSource] = None,
    ) -> Optional["ContractYaml"]:
        source = contract_yaml_source if contract_yaml_source is not None else check_collection_yaml_source
        return super().parse(
            check_collection_yaml_source=source,
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
        source = contract_yaml_source if contract_yaml_source is not None else check_collection_yaml_source
        super().__init__(
            check_collection_yaml_source=source,
            provided_variable_values=provided_variable_values,
            data_timestamp=data_timestamp,
            primary_data_source_impl=primary_data_source_impl,
        )

    @property
    def contract_yaml_source(self):
        """Backwards-compatible alias for self.check_collection_yaml_source."""
        return self.check_collection_yaml_source

    @property
    def contract_yaml_object(self):
        """Backwards-compatible alias for self.check_collection_yaml_object."""
        return self.check_collection_yaml_object


# Wire the polymorphism hook: when ``CheckCollectionYaml.parse(...)`` is called on
# the base directly, the base resolves ``_YAML_CLASS`` and constructs ContractYaml.
# A future ``DataStandardYaml`` subclass would set its own ``_YAML_CLASS`` to itself
# below its class definition and have the same base-layer code construct its type
# instead.
ContractYaml._YAML_CLASS = ContractYaml


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
