"""
Helpers for CheckImpl tests.

Builds ContractImpl objects via the same path as production code, but without
a database connection (only_validate_without_execute=True).  This gives tests
real CheckImpl instances whose setup_metrics() has already been called.

The module also provides helpers for constructing MeasurementValues so that
evaluate() can be tested without a database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from helpers.test_functions import dedent_and_strip
from soda_core.common.logging_constants import (  # noqa: F401 – side-effect import
    Emoticons,
)
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    Measurement,
)
from soda_core.contracts.impl.contract_verification_impl import (
    CheckImpl,
    ContractImpl,
    ContractYaml,
    DerivedMetricImpl,
    MeasurementValues,
    MetricImpl,
)

# ---------------------------------------------------------------------------
# ContractImpl construction
# ---------------------------------------------------------------------------


def build_contract_impl(yaml_str: str, variables: Optional[dict[str, str]] = None) -> ContractImpl:
    """
    Build a *real* ContractImpl from a contract YAML string.

    Uses ``only_validate_without_execute=True`` so that no database connection
    is required.  The returned ``ContractImpl`` has its ``check_impls`` fully
    initialised (including ``setup_metrics()`` having been called).
    """
    contract_yaml_source = ContractYamlSource.from_str(dedent_and_strip(yaml_str))
    contract_yaml: ContractYaml = ContractYaml.parse(
        contract_yaml_source=contract_yaml_source,
        provided_variable_values=variables or {},
    )
    logs = Logs()
    now = datetime.now(tz=timezone.utc)
    contract_impl = ContractImpl(
        logs=logs,
        contract_yaml=contract_yaml,
        only_validate_without_execute=True,
        data_source_impl=None,
        all_data_source_impls={},
        data_timestamp=contract_yaml.data_timestamp or now,
        execution_timestamp=contract_yaml.execution_timestamp or now,
        soda_cloud=None,
        publish_results=False,
    )
    return contract_impl


def get_check_impl(yaml_str: str, check_index: int = 0, variables: Optional[dict[str, str]] = None) -> CheckImpl:
    """
    Build a ContractImpl and return a specific CheckImpl by index.

    ``check_index`` indexes into ``contract_impl.all_check_impls``.
    """
    contract_impl = build_contract_impl(yaml_str, variables=variables)
    return contract_impl.all_check_impls[check_index]


# ---------------------------------------------------------------------------
# MeasurementValues construction
# ---------------------------------------------------------------------------


def build_measurement_values(
    metric_values: list[tuple[MetricImpl, object]],
    contract_impl: Optional[ContractImpl] = None,
) -> MeasurementValues:
    """
    Build a ``MeasurementValues`` from a list of (MetricImpl, value) pairs.

    Uses a list of tuples instead of a dict because MetricImpl defines
    ``__eq__`` without ``__hash__``, making it unhashable.

    If *contract_impl* is provided the helper will also derive all
    ``DerivedMetricImpl`` values (e.g. percentages).
    """
    measurements = [
        Measurement(metric_id=metric.id, value=value, metric_name=metric.type) for metric, value in metric_values
    ]
    mv = MeasurementValues(measurements)

    if contract_impl is not None:
        for metric in contract_impl.metrics:
            if isinstance(metric, DerivedMetricImpl):
                mv.derive_value(metric)

    return mv


# ---------------------------------------------------------------------------
# Legacy helper (kept for backward-compat with existing YAML-validation tests)
# ---------------------------------------------------------------------------


def validate_contract(yaml_str: str, variables: Optional[dict[str, str]] = None):
    """
    Run contract validation (without execution) and return the session result.
    This builds ContractImpl objects but does not run any SQL queries.
    """
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(yaml_str))],
        only_validate_without_execute=True,
        variables=variables,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )
    return result
