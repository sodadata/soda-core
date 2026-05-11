from __future__ import annotations

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
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.sql_dialect import *
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


class ContractVerificationSessionImpl(CheckCollectionVerificationSessionImpl):
    @classmethod
    def execute(
        cls,
        contract_yaml_sources: Optional[list[ContractYamlSource]] = None,
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        data_source_impls: Optional[list[DataSourceImpl]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional[SodaCloud] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
        check_selectors: Optional[list[CheckSelector]] = None,
        dwh_data_source_file_path: Optional[str] = None,
        check_collection_yaml_sources: Optional[list[CheckCollectionYamlSource]] = None,
    ):
        sources = contract_yaml_sources if contract_yaml_sources is not None else check_collection_yaml_sources
        return super().execute(
            check_collection_yaml_sources=sources,
            only_validate_without_execute=only_validate_without_execute,
            variables=variables,
            data_timestamp=data_timestamp,
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            soda_cloud_impl=soda_cloud_impl,
            soda_cloud_publish_results=soda_cloud_publish_results,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_verbose=soda_cloud_verbose,
            soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
            check_selectors=check_selectors,
            dwh_data_source_file_path=dwh_data_source_file_path,
        )


class ContractImpl(CheckCollectionImpl):
    @property
    def contract_yaml(self) -> ContractYaml:
        return self.check_collection_yaml

    def __init__(
        self,
        logs: Logs,
        check_collection_yaml: Optional[ContractYaml] = None,
        only_validate_without_execute: bool = False,
        data_source_impl: Optional[DataSourceImpl] = None,
        all_data_source_impls: Optional[dict[str, DataSourceImpl]] = None,
        data_timestamp: Optional[datetime] = None,
        execution_timestamp: Optional[datetime] = None,
        soda_cloud: Optional[SodaCloud] = None,
        publish_results: bool = False,
        check_selectors: list[CheckSelector] = [],
        dwh_data_source_file_path: Optional[str] = None,
        contract_yaml: Optional[ContractYaml] = None,
    ):
        resolved_yaml = check_collection_yaml if check_collection_yaml is not None else contract_yaml
        super().__init__(
            logs=logs,
            check_collection_yaml=resolved_yaml,
            only_validate_without_execute=only_validate_without_execute,
            data_source_impl=data_source_impl,
            all_data_source_impls=all_data_source_impls if all_data_source_impls is not None else {},
            data_timestamp=data_timestamp,
            execution_timestamp=execution_timestamp,
            soda_cloud=soda_cloud,
            publish_results=publish_results,
            check_selectors=check_selectors,
            dwh_data_source_file_path=dwh_data_source_file_path,
        )


# Wire the polymorphism hooks on the Contract* subtypes. The base classes
# (CheckCollectionVerificationSessionImpl, CheckCollectionImpl) read these at
# construction sites and route to the concrete classes. A future
# DataStandard* subtype would subclass these and set its own _YAML_CLASS /
# _IMPL_CLASS / _RESULT_CLASS / _SESSION_RESULT_CLASS to its own types.
ContractVerificationSessionImpl._YAML_CLASS = ContractYaml
ContractVerificationSessionImpl._IMPL_CLASS = ContractImpl
ContractVerificationSessionImpl._RESULT_CLASS = ContractVerificationResult
ContractVerificationSessionImpl._SESSION_RESULT_CLASS = ContractVerificationSessionResult

ContractImpl._RESULT_CLASS = ContractVerificationResult


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
