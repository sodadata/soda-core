from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from soda_core.check_collections.impl.check_collection_verification_impl import (
    AggregationMetricImpl,
    AggregationQuery,
    CheckCollectionImpl,
    CheckCollectionImplExtension,
    CheckCollectionVerificationSessionImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractVerificationHandler,
    ContractVerificationHandlerRegistry,
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
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import (
    CheckCollectionYamlSource,
    ContractYamlSource,
    DataSourceYamlSource,
)
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    ContractVerificationSessionResult,
)
from soda_core.contracts.impl.check_selector import CheckSelector
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


class ContractImpl(
    CheckCollectionImpl[ContractYaml, ContractVerificationResult],
    result_type=ContractVerificationResult,
):
    @property
    def contract_yaml(self) -> ContractYaml:
        return self.check_collection_yaml


class ContractVerificationSessionImpl(
    CheckCollectionVerificationSessionImpl[ContractYaml, ContractImpl, ContractVerificationSessionResult],
    yaml_type=ContractYaml,
    impl_type=ContractImpl,
    session_result_type=ContractVerificationSessionResult,
):
    pass


# Re-exports for backwards compatibility
__all__ = [
    "ContractImpl",
    "ContractVerificationSessionImpl",
    "CheckCollectionImpl",
    "CheckCollectionImplExtension",
    "CheckCollectionVerificationSessionImpl",
    "ContractVerificationHandler",
    "ContractVerificationHandlerRegistry",
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
