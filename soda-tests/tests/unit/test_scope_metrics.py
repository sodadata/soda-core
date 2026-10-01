"""Scope activation in core: the hook that activates declared scopes, the scope and id of each metric, the base
selection and bundling of aggregation metrics, the filtered CTE sampler and the scope rows tested.

``helpers.scope_activation_extension`` stands in for the module that runs declared scopes. An unscoped contract
must keep every metric id, every identity and its CTE, with and without runner sampling.

Every test here drops the soda-scopes extension, so only the stand-in activates scopes even with soda-scopes installed.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Optional
from unittest import mock

import duckdb
import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from helpers.scope_activation_extension import scope_activation
from helpers.scope_test_kinds import ScopeUnsupportedImpl
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_functions import dedent_and_strip
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.filtered_cte import build_filtered_cte
from soda_core.common.logs import Logs
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO, TestRowSamplerConfigurationDTO
from soda_core.common.sql_ast import COUNT, SODA_FILTERED_CTE_NAME, STAR, SqlExpression
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    ContractVerificationSession,
    Measurement,
)
from soda_core.contracts.impl.check_types.missing_check import MissingCountMetricImpl
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    AggregationQuery,
    CheckCollectionImplExtension,
    ContractImpl,
    DerivedPercentageMetricImpl,
    MeasurementValues,
    MetricImpl,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.contracts.impl.scope import Scope
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

SCOPED_YAML: str = """
    dataset: fx/main/orders
    filter: id > 0
    scopes:
      eu: {name: EU, filter: region = 'eu'}
      us: {name: US, filter: region = 'us'}
      apac: {name: APAC}
    columns:
      - name: email
        checks:
          - missing:
          - missing: {qualifier: eu, scope: eu}
          - missing: {qualifier: us, scope: us}
          - missing: {qualifier: apac, scope: apac}
    checks:
      - row_count:
      - row_count: {qualifier: eu, scope: eu}
"""


def _build_impl(
    yaml_str: str,
    impl_class: type[CheckCollectionImpl] = ContractImpl,
    data_source_impl: Optional[DuckDBDataSourceImpl] = None,
    soda_cloud: Optional[MockSodaCloud] = None,
) -> tuple[CheckCollectionImpl, Logs]:
    """``impl_class`` built from ``yaml_str``, without executing, and the closed logs of the build."""
    logs = Logs()
    try:
        yaml = ContractYaml.parse(yaml_source=ContractYamlSource.from_str(dedent_and_strip(yaml_str)))
        impl = impl_class(
            logs=logs,
            yaml=yaml,
            only_validate_without_execute=data_source_impl is None,
            data_source_impl=data_source_impl,
            soda_cloud_impl=soda_cloud,
        )
        return impl, logs
    finally:
        logs.close()


def _duckdb_data_source() -> DuckDBDataSourceImpl:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE orders (id INTEGER, region VARCHAR, email VARCHAR)")
    connection.execute(
        "INSERT INTO orders VALUES (1, 'eu', NULL), (2, 'eu', 'a'), (3, 'us', NULL), (4, 'us', NULL), (5, 'us', 'b')"
    )
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="fx")


def _checks_by_qualifier(impl: CheckCollectionImpl, check_type: str) -> dict:
    return {
        check_impl.check_yaml.qualifier: check_impl
        for check_impl in impl.all_check_impls
        if check_impl.type == check_type
    }


class _TestAggregationMetric(AggregationMetricImpl):
    def sql_expression(self) -> SqlExpression:
        return COUNT(STAR())

    def sql_condition_expression(self) -> Optional[SqlExpression]:
        return None


def test_metric_scope_defaults_to_none_and_the_keyword_sets_it():
    impl, _ = _build_impl(SCOPED_YAML)
    eu = impl.scopes["eu"]
    assert RowCountMetricImpl(contract_impl=impl).scope is None
    assert RowCountMetricImpl(contract_impl=impl, scope=eu).scope is eu
    assert _TestAggregationMetric(contract_impl=impl, metric_type="test", scope=eu).scope is eu


def test_the_base_scope_adds_no_id_term():
    impl, _ = _build_impl(SCOPED_YAML)
    unscoped = RowCountMetricImpl(contract_impl=impl)
    base = RowCountMetricImpl(contract_impl=impl, scope=impl.base_scope)

    assert list(unscoped._get_id_properties())[0] == "type"
    assert base._get_id_properties() == unscoped._get_id_properties()
    assert base.id == unscoped.id == impl.row_count_metric_impl.id


def test_a_declared_scope_is_the_first_id_term():
    impl, _ = _build_impl(SCOPED_YAML)
    unscoped = RowCountMetricImpl(contract_impl=impl)
    scoped = RowCountMetricImpl(contract_impl=impl, scope=impl.scopes["eu"])

    assert list(scoped._get_id_properties().items()) == [("scope", "eu:")] + list(unscoped._get_id_properties().items())
    assert scoped.id != unscoped.id
    assert scoped.id != RowCountMetricImpl(contract_impl=impl, scope=impl.scopes["us"]).id


def test_the_scoping_step_scopes_metrics_on_the_collection_dataset_only():
    with scope_activation("eu"):
        impl, logs = _build_impl(SCOPED_YAML)
    assert not logs.has_errors
    row_count_checks = _checks_by_qualifier(impl, "row_count")
    base_check, eu_check = row_count_checks[None], row_count_checks["eu"]
    eu = impl.scopes["eu"]

    # A base check sets the scope and keeps the id.
    metric = RowCountMetricImpl(contract_impl=impl)
    unscoped_id = metric.id
    assert base_check.apply_scope_to_metric(metric) is metric
    assert metric.scope is impl.base_scope and metric.id == unscoped_id

    # A check in a declared scope rebuilds the id with the scope term, once.
    metric = RowCountMetricImpl(contract_impl=impl)
    assert eu_check.apply_scope_to_metric(metric) is metric
    assert metric.scope is eu
    assert metric.id == RowCountMetricImpl(contract_impl=impl, scope=eu).id != unscoped_id
    scoped_id = metric.id
    assert eu_check.apply_scope_to_metric(metric) is metric and metric.id == scoped_id
    assert base_check.apply_scope_to_metric(metric) is metric and metric.scope is eu and metric.id == scoped_id

    # A metric that already has a scope, the base included, comes back untouched.
    us_metric = RowCountMetricImpl(contract_impl=impl, scope=impl.scopes["us"])
    us_id = us_metric.id
    eu_check.apply_scope_to_metric(us_metric)
    assert us_metric.scope is impl.scopes["us"] and us_metric.id == us_id
    base_metric = RowCountMetricImpl(contract_impl=impl, scope=impl.base_scope)
    eu_check.apply_scope_to_metric(base_metric)
    assert base_metric.scope is impl.base_scope and base_metric.id == unscoped_id

    # The gate compares the dataset object, so a metric on another dataset stays unscoped, even with equal text.
    for dataset_identifier in [DatasetIdentifier.parse("fx/main/other"), DatasetIdentifier.parse("fx/main/orders")]:
        other_metric = RowCountMetricImpl(contract_impl=impl, dataset_identifier=dataset_identifier)
        other_id = other_metric.id
        eu_check.apply_scope_to_metric(other_metric)
        assert other_metric.scope is None and other_metric.id == other_id


def test_resolving_a_base_metric_keeps_an_id_suffix_added_before_it():
    # An extension may extend a metric id before it resolves the metric, so the base must keep the id as it is.
    impl, _ = _build_impl(SCOPED_YAML)
    base_check = _checks_by_qualifier(impl, "row_count")[None]
    metric = RowCountMetricImpl(contract_impl=impl, filter="id > 2")
    metric.id = f"{metric.id}-suffix"

    resolved = base_check._resolve_metric(metric)

    assert resolved is metric and metric.id.endswith("-suffix")
    assert metric.scope is impl.base_scope
    assert base_check.metrics[-1] is metric


def test_identical_metrics_in_two_scopes_do_not_merge_in_the_resolver():
    with scope_activation("eu", "us"):
        impl, logs = _build_impl(SCOPED_YAML)
    assert not logs.has_errors
    missing_checks = _checks_by_qualifier(impl, "missing")

    missing_count_metrics = [
        metric for metric in impl.metrics_resolver.get_resolved_metrics() if isinstance(metric, MissingCountMetricImpl)
    ]
    assert [metric.scope for metric in missing_count_metrics] == [
        impl.base_scope,
        impl.scopes["eu"],
        impl.scopes["us"],
    ]
    assert len({metric.id for metric in missing_count_metrics}) == 3
    for key in ["eu", "us"]:
        check_impl = missing_checks[key]
        assert check_impl.missing_count_metric_impl is missing_count_metrics[["eu", "us"].index(key) + 1]
        # The check's row count is the scope's own row count metric.
        assert check_impl.row_count_metric_impl is impl.scopes[key].row_count_metric
    assert missing_checks[None].row_count_metric_impl is impl.row_count_metric_impl
    # The declared scope nobody activated builds no metrics.
    assert missing_checks["apac"].skip and not missing_checks["apac"].metrics


def test_the_missing_percentage_metric_goes_through_the_check():
    with scope_activation("eu"):
        impl, _ = _build_impl(SCOPED_YAML)
    missing_checks = _checks_by_qualifier(impl, "missing")

    for key in [None, "eu"]:
        check_impl = missing_checks[key]
        percent_metric = check_impl.missing_percent_metric_impl
        assert percent_metric in check_impl.metrics
        assert percent_metric.scope is check_impl.scope
    base_check = missing_checks[None]
    direct = DerivedPercentageMetricImpl(
        metric_type="missing_percent",
        fraction_metric_impl=base_check.missing_count_metric_impl,
        total_metric_impl=base_check.row_count_metric_impl,
    )
    assert base_check.missing_percent_metric_impl.id == direct.id
    assert missing_checks["eu"].missing_percent_metric_impl.id != direct.id


def test_activation_runs_before_the_columns_are_parsed():
    with scope_activation("eu") as activated_impls:
        impl, logs = _build_impl(SCOPED_YAML)
    assert activated_impls == [impl]
    assert not logs.has_errors

    eu = impl.scopes["eu"]
    assert eu.is_active and eu.cte.alias == "_soda_filtered_scope_eu"
    assert eu.row_count_metric.scope is eu
    assert eu.row_count_metric in impl.metrics_resolver.get_resolved_metrics()
    # A column check and a dataset check in the activated scope run; the other scopes stay inactive.
    assert not _checks_by_qualifier(impl, "missing")["eu"].skip
    assert not _checks_by_qualifier(impl, "row_count")["eu"].skip
    assert _checks_by_qualifier(impl, "missing")["us"].skip
    assert not impl.scopes["us"].is_active and not impl.scopes["apac"].is_active


def test_without_activation_every_declared_scope_stays_inactive():
    impl, logs = _build_impl(SCOPED_YAML)
    assert not logs.has_errors
    assert not any(scope.is_active for scope in impl.scopes.values())
    assert [check_impl.skip for check_impl in impl.all_check_impls if check_impl.scope.is_base] == [False, False]
    assert all(check_impl.skip for check_impl in impl.all_check_impls if not check_impl.scope.is_base)


def test_a_kind_without_scope_support_never_activates_scopes():
    with scope_activation("eu", impl_class=ScopeUnsupportedImpl) as activated_impls:
        impl, logs = _build_impl(SCOPED_YAML, impl_class=ScopeUnsupportedImpl)
    assert activated_impls == []
    assert not impl.scopes["eu"].is_active
    assert _checks_by_qualifier(impl, "missing")["eu"].skip
    assert not logs.has_errors


def test_the_default_hook_does_nothing():
    impl, _ = _build_impl(SCOPED_YAML)
    extension = SimpleNamespace()
    assert CheckCollectionImplExtension.activate_scopes(extension, contract_impl=impl) is None


def test_a_failing_activation_logs_an_error():
    class _FailingExtension(CheckCollectionImplExtension):
        def __init__(self, contract_impl: CheckCollectionImpl):
            self.contract_impl = contract_impl

        def activate_scopes(self, contract_impl: CheckCollectionImpl) -> None:
            raise RuntimeError("boom")

    ContractImpl.register_extension("failing_scope_activation", _FailingExtension)
    try:
        impl, logs = _build_impl(SCOPED_YAML)
    finally:
        ContractImpl.impl_extensions.pop("failing_scope_activation", None)

    assert logs.has_errors
    assert "Error activating scopes with extension _FailingExtension: boom" in logs.get_errors()



def _nudge_lines(logs: Logs) -> list[str]:
    return [line for line in logs.get_logs() if "needs a Soda extension that runs scopes" in line]


def test_no_nudge_after_an_extension_that_runs_scopes_failed_to_activate_them():
    class _RaisingActivation(CheckCollectionImplExtension):
        def __init__(self, contract_impl: CheckCollectionImpl):
            self.contract_impl = contract_impl

        def activate_scopes(self, contract_impl: CheckCollectionImpl) -> None:
            raise RuntimeError("boom")

    class _ActivatesNothing(_RaisingActivation):
        def activate_scopes(self, contract_impl: CheckCollectionImpl) -> None:
            return None

    for extension_class in (_RaisingActivation, _ActivatesNothing):
        ContractImpl.register_extension("scope_activation_that_activates_nothing", extension_class)
        try:
            impl, logs = _build_impl(SCOPED_YAML)
        finally:
            ContractImpl.impl_extensions.pop("scope_activation_that_activates_nothing", None)
        assert [check_impl.skip for check_impl in impl.all_check_impls] == [False, True, True, True, False, True]
        assert _nudge_lines(logs) == [], extension_class.__name__


def test_an_extension_that_does_not_run_scopes_keeps_the_nudge():
    """It inherits the default activate_scopes, like an extension that only parses its own checks."""

    class _ParsesChecksOnly(CheckCollectionImplExtension):
        def __init__(self, contract_impl: CheckCollectionImpl):
            self.contract_impl = contract_impl

    ContractImpl.register_extension("extension_that_does_not_run_scopes", _ParsesChecksOnly)
    try:
        _, logs = _build_impl(SCOPED_YAML)
    finally:
        ContractImpl.impl_extensions.pop("extension_that_does_not_run_scopes", None)
    assert _nudge_lines(logs) == [
        "Excluded 4 checks whose scope is not active. Running checks in a scope needs a Soda extension that runs scopes."
    ]

SAMPLING_YAML: str = """
    dataset: fx/main/orders
    filter: id > 0
    check_attributes: {team: data}
    scopes:
      eu: {name: EU, filter: region = 'eu'}
    columns:
      - name: email
        checks:
          - missing:
          - missing: {qualifier: eu, scope: eu}
    checks:
      - row_count:
"""


def _sampling_soda_cloud() -> MockSodaCloud:
    soda_cloud = MockSodaCloud()
    soda_cloud.set_dataset_configuration_response(
        dataset_identifier=DatasetIdentifier.parse("fx/main/orders"),
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": "absoluteLimit", "limit": 3}
            )
        ),
    )
    return soda_cloud


def test_the_base_cte_comes_from_the_factory():
    impl, _ = _build_impl(SAMPLING_YAML, soda_cloud=_sampling_soda_cloud())

    assert impl.filtered_cte_sampler is None
    assert impl.cte == build_filtered_cte(impl.dataset_identifier, "id > 0", SODA_FILTERED_CTE_NAME)
    assert impl.base_scope.cte is impl.cte


@mock.patch.object(EnvConfigHelper, "is_contract_test_scan_definition_type", new_callable=mock.PropertyMock)
@mock.patch.object(EnvConfigHelper, "is_running_on_runner", new_callable=mock.PropertyMock)
def test_runner_sampling_samples_the_base_cte_and_keeps_every_identity(
    is_running_on_runner, is_contract_test_scan_definition_type, caplog
):
    is_running_on_runner.return_value = False
    is_contract_test_scan_definition_type.return_value = False
    with scope_activation("eu"):
        unsampled, _ = _build_impl(SAMPLING_YAML, soda_cloud=_sampling_soda_cloud())

    is_running_on_runner.return_value = True
    is_contract_test_scan_definition_type.return_value = True
    with caplog.at_level(logging.INFO), scope_activation("eu"):
        impl, logs = _build_impl(SAMPLING_YAML, soda_cloud=_sampling_soda_cloud())
    assert not logs.has_errors
    assert "Row sampling is enabled for dataset fx/main/orders" in caplog.text

    sampler = (SamplerType.ABSOLUTE_LIMIT, 3)
    assert impl.filtered_cte_sampler == sampler
    assert impl.cte == build_filtered_cte(impl.dataset_identifier, "id > 0", SODA_FILTERED_CTE_NAME, sampler)
    assert impl.cte != unsampled.cte
    eu = impl.scopes["eu"]
    assert eu.cte == build_filtered_cte(impl.dataset_identifier, "region = 'eu'", "_soda_filtered_scope_eu", sampler)

    # The skeleton's base scope asserts hold with sampling on.
    base_scope = impl.base_scope
    assert base_scope.cte is impl.cte and base_scope.row_count_metric is impl.row_count_metric_impl
    assert base_scope.filter is impl.filter and base_scope.check_attributes is impl.check_attributes
    assert base_scope.cte_alias() == impl.cte.alias == SODA_FILTERED_CTE_NAME

    # Sampling changes no identity and no metric id.
    assert [check_impl.identity for check_impl in impl.all_check_impls] == [
        check_impl.identity for check_impl in unsampled.all_check_impls
    ]
    assert [metric.id for metric in impl.metrics_resolver.get_resolved_metrics()] == [
        metric.id for metric in unsampled.metrics_resolver.get_resolved_metrics()
    ]


BUNDLING_YAML: str = """
    dataset: fx/main/orders
    scopes:
      eu: {name: EU, filter: region = 'eu'}
      us: {name: US, filter: region = 'us'}
    columns:
      - name: email
        checks:
          - missing:
          - missing: {qualifier: eu, scope: eu}
          - invalid: {qualifier: eu, scope: eu, valid_values: ['a', 'b']}
          - missing: {qualifier: us, scope: us}
"""


def _aggregation_queries(impl: CheckCollectionImpl, scope: Scope) -> list[AggregationQuery]:
    return [query for query in impl.queries if isinstance(query, AggregationQuery) and query.cte is scope.cte]


def test_the_base_bundles_only_its_own_metrics():
    with scope_activation("eu", "us"):
        impl, logs = _build_impl(BUNDLING_YAML, data_source_impl=_duckdb_data_source())
    assert not logs.has_errors

    [base_query] = _aggregation_queries(impl, impl.base_scope)
    assert all(metric.scope is None or metric.scope.is_base for metric in base_query.aggregation_metrics)
    assert "_soda_filtered_scope_" not in base_query.sql

    for key in ["eu", "us"]:
        scope = impl.scopes[key]
        [scope_query] = _aggregation_queries(impl, scope)
        assert all(metric.scope is scope for metric in scope_query.aggregation_metrics)
        assert f'"_soda_filtered_scope_{key}" AS (' in scope_query.sql
        assert f"region = '{key}'" in scope_query.sql
        assert scope.row_count_metric in scope_query.aggregation_metrics


def test_bundling_dedupes_within_one_scope_and_never_across_scopes():
    with scope_activation("eu", "us"):
        impl, _ = _build_impl(BUNDLING_YAML, data_source_impl=_duckdb_data_source())
    eu, us = impl.scopes["eu"], impl.scopes["us"]

    # Missing and invalid in one scope both resolve a missing count that renders the same SQL: one is aliased.
    [eu_query] = _aggregation_queries(impl, eu)
    eu_missing_counts = [
        metric for metric in eu_query.aggregation_metrics if isinstance(metric, MissingCountMetricImpl)
    ]
    assert len(eu_missing_counts) == 1
    assert [len(aliases) for aliases in eu_query.metric_aliases.values()] == [1]

    # Metrics of two scopes that render the same SQL each land in their own scope's query.
    us_metric = _checks_by_qualifier(impl, "missing")["us"].missing_count_metric_impl
    [us_query] = _aggregation_queries(impl, us)
    assert us_metric in us_query.aggregation_metrics and us_metric not in eu_query.aggregation_metrics
    assert us_query.metric_aliases == {}

    queries = impl.bundle_aggregation_metrics([eu.row_count_metric, eu_missing_counts[0]], eu)
    assert [query.cte for query in queries] == [eu.cte]
    assert queries[0].aggregation_metrics == [eu.row_count_metric, eu_missing_counts[0]]


def test_bundling_requires_an_active_scope():
    impl, _ = _build_impl(BUNDLING_YAML, data_source_impl=_duckdb_data_source())
    with pytest.raises(ValueError, match="inactive scope 'eu'"):
        impl.bundle_aggregation_metrics([], impl.scopes["eu"])


def _check_stub(scope: Scope, metrics: list) -> SimpleNamespace:
    return SimpleNamespace(scope=scope, metrics=metrics)


def test_scope_rows_tested_goes_only_on_a_check_that_aggregates_in_its_own_scope():
    with scope_activation("eu"):
        impl, _ = _build_impl(SCOPED_YAML)
    eu = impl.scopes["eu"]
    eu_metric = RowCountMetricImpl(contract_impl=impl, scope=eu, filter="id > 1")
    base_metric = RowCountMetricImpl(contract_impl=impl, filter="id > 1")
    measurement_values = MeasurementValues(
        [
            Measurement(metric_id=eu.row_count_metric.id, value=2, metric_name="row_count"),
            Measurement(metric_id=impl.row_count_metric_impl.id, value=5, metric_name="row_count"),
        ]
    )

    def _diagnostics(
        check_impl, diagnostic_metric_values, values: MeasurementValues = measurement_values
    ) -> Optional[dict]:
        check_result = CheckResult(
            check=None, outcome=CheckOutcome.PASSED, diagnostic_metric_values=diagnostic_metric_values
        )
        impl._add_scope_rows_tested(check_impl, check_result, values)
        return check_result.diagnostic_metric_values

    assert _diagnostics(_check_stub(eu, [eu_metric]), {"dataset_rows_tested": 5}) == {
        "dataset_rows_tested": 5,
        "scope_rows_tested": 2,
    }
    # The base scope and a scope without its own row count get nothing.
    assert _diagnostics(_check_stub(impl.base_scope, [base_metric]), {"dataset_rows_tested": 5}) == {
        "dataset_rows_tested": 5
    }
    no_row_count = Scope(key="us")
    no_row_count.activate(cte=eu.cte, row_count_metric=None)
    assert _diagnostics(_check_stub(no_row_count, [eu_metric]), {"dataset_rows_tested": 5}) == {
        "dataset_rows_tested": 5
    }
    # A check that aggregates nothing in its scope, like a query-form check, gets nothing.
    query_metric = MetricImpl(contract_impl=impl, metric_type="metric_query", scope=eu)
    assert _diagnostics(_check_stub(eu, [query_metric]), {"dataset_rows_tested": 5}) == {"dataset_rows_tested": 5}
    assert _diagnostics(_check_stub(eu, [base_metric]), {"dataset_rows_tested": 5}) == {"dataset_rows_tested": 5}
    # Diagnostics without the dataset count, empty ones included, and diagnostics that are not a dict stay as they are.
    assert _diagnostics(_check_stub(eu, [eu_metric]), {}) == {}
    assert _diagnostics(_check_stub(eu, [eu_metric]), {"target_dataset_rows_tested": 5}) == {
        "target_dataset_rows_tested": 5
    }
    assert _diagnostics(_check_stub(eu, [eu_metric]), None) is None
    # An unmeasured scope count is still a key.
    assert _diagnostics(_check_stub(eu, [eu_metric]), {"dataset_rows_tested": None}, MeasurementValues([])) == {
        "dataset_rows_tested": None,
        "scope_rows_tested": None,
    }


def _verify_on_duckdb(yaml_str: str, *scope_keys: str):
    data_source_impl = _duckdb_data_source()
    with scope_activation(*scope_keys):
        session_result = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(yaml_str))],
            data_source_impls=[data_source_impl],
        )
    [result] = session_result.contract_verification_results
    return result


def test_scope_rows_tested_on_evaluated_and_unmeasured_checks():
    result = _verify_on_duckdb(
        """
        dataset: fx/main/orders
        scopes:
          eu: {name: EU, filter: region = 'eu'}
          us: {name: US, filter: region = 'us'}
        columns:
          - name: email
            checks:
              - missing:
              - missing: {qualifier: eu, scope: eu, threshold: {must_be_less_than: 5}}
          - name: missing_column
            checks:
              - missing: {qualifier: us, scope: us}
        """,
        "eu",
        "us",
    )

    diagnostics = {
        (check_result.check.scope, check_result.outcome): check_result.diagnostic_metric_values
        for check_result in result.check_results
    }
    assert diagnostics[(None, CheckOutcome.FAILED)] == {
        "missing_count": 3,
        "missing_percent": 60.0,
        "check_rows_tested": 5,
        "dataset_rows_tested": 5,
    }
    assert diagnostics[("eu", CheckOutcome.PASSED)] == {
        "missing_count": 1,
        "missing_percent": 50.0,
        "check_rows_tested": 2,
        "dataset_rows_tested": 5,
        "scope_rows_tested": 2,
    }
    # The us query fails on the column the table lacks, so the us check is not evaluated and its scope count is
    # unmeasured.
    assert diagnostics[("us", CheckOutcome.NOT_EVALUATED)] == {"dataset_rows_tested": 5, "scope_rows_tested": None}
