from __future__ import annotations

import logging

from soda_core.check_collections.impl.check_collection_verification_impl import (
    AggregationMetricImpl,
    AggregationQuery,
    CheckCollectionImpl,
    CheckCollectionImplExtension,
    CheckCollectionVerificationSessionImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    DerivedMetricImpl,
    DerivedPercentageMetricImpl,
    MeasurementValues,
    MetricImpl,
    MetricsResolver,
    MissingAndValidity,
    MissingAndValidityCheckImpl,
    Query,
    ThresholdImpl,
    ThresholdLevel,
    ThresholdType,
    ValidCountMetric,
    ValidReferenceData,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    ContractVerificationSessionResult,
)
from soda_core.contracts.impl.contract_yaml import (
    CheckYaml,
    ColumnYaml,
    ContractYaml,
    MissingAncValidityCheckYaml,
    MissingAndValidityYaml,
    RegexFormat,
    ThresholdYaml,
    ValidReferenceDataYaml,
)

logger: logging.Logger = soda_logger


# ContractImpl is declared before ContractVerificationSessionImpl because the
# session impl binds its Generic args at class-creation time (not via a
# deferred annotation).
class ContractImpl(CheckCollectionImpl[ContractYaml, ContractVerificationResult]):
    _DISPLAY_NAME = "contract"

    @property
    def is_test_verification_on_agent(self) -> bool:
        return self.soda_config.is_running_on_agent and self.soda_config.is_contract_test_scan_definition_type

    @property
    def contract_yaml(self) -> ContractYaml:
        return self.check_collection_yaml


class ContractVerificationSessionImpl(
    CheckCollectionVerificationSessionImpl[ContractYaml, ContractImpl, ContractVerificationSessionResult]
):
    pass


# Re-exports for backwards compatibility
__all__ = [
    "ContractImpl",
    "ContractVerificationSessionImpl",
    "CheckCollectionImpl",
    "CheckCollectionImplExtension",
    "CheckCollectionVerificationSessionImpl",
    "MeasurementValues",
    "ColumnImpl",
    "ValidReferenceData",
    "MissingAndValidity",
    "MetricsResolver",
    "ThresholdType",
    "ThresholdLevel",
    "ThresholdImpl",
    "CheckParser",
    "CheckImpl",
    "MissingAndValidityCheckImpl",
    "MetricImpl",
    "AggregationMetricImpl",
    "DerivedMetricImpl",
    "DerivedPercentageMetricImpl",
    "ValidCountMetric",
    "Query",
    "AggregationQuery",
    "ContractYaml",
    "CheckYaml",
    "ColumnYaml",
    "MissingAncValidityCheckYaml",
    "MissingAndValidityYaml",
    "RegexFormat",
    "ThresholdYaml",
    "ValidReferenceDataYaml",
]
