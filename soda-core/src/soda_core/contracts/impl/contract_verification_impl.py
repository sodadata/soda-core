from __future__ import annotations

import logging

from soda_core.check_collections.check_collection_family import (
    CheckCollectionFamily,
    register_family,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    AggregationMetricImpl,
    AggregationQuery,
    CheckCollectionImpl,
    CheckCollectionImplExtension,
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
from soda_core.contracts.contract_verification import ContractVerificationResult
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


class ContractImpl(CheckCollectionImpl[ContractYaml, ContractVerificationResult]):
    _DISPLAY_NAME = "contract"
    _WIRE_SOURCE = "soda-contract"
    _TEST_SCAN_DEFINITION_TYPE = "contractTest"

    @property
    def contract_yaml(self) -> ContractYaml:
        return self.check_collection_yaml


def _verify_contract_on_agent(
    soda_cloud_impl,
    check_collection_yaml,
    variables,
    blocking_timeout_in_minutes,
    publish_results,
    verbose,
):
    """Module-level agent verifier registered on the ``contract`` family.

    Replaces the previous ``ContractVerificationSessionImpl._verify_on_agent``
    classmethod hook — the family registry now dispatches per-spec.
    """
    return soda_cloud_impl.verify_contract_on_agent(
        contract_yaml=check_collection_yaml,
        variables=variables,
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        publish_results=publish_results,
        verbose=verbose,
    )


# Register the contract family at module-import time. The session impl reads
# each spec's ``kind`` and dispatches via the registry; importing this module
# is what wires the contract subtype. Identity (display name, wire source,
# test scan-definition type, result class) is read off ``ContractImpl``'s
# ClassVars at dispatch time — the family carries only the dispatcher's
# minimal needs.
register_family(
    CheckCollectionFamily(
        kind="contract",
        yaml_class=ContractYaml,
        impl_class=ContractImpl,
        on_agent_verifier=_verify_contract_on_agent,
    )
)


# Re-exports for backwards compatibility.
# ``CheckCollectionVerificationSessionImpl`` lives in
# ``check_collections.impl.check_collection_verification_impl`` — import it
# from there directly. Re-exporting it here is misleading now that the
# universal session impl is family-agnostic.
__all__ = [
    "ContractImpl",
    "CheckCollectionImpl",
    "CheckCollectionImplExtension",
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
