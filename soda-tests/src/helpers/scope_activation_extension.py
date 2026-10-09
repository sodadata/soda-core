"""A test-only stand-in for the module that runs declared scopes.

``scope_activation(*keys)`` registers an extension on a check-collection kind for the length of a ``with``
block. The extension activates the named scopes, each on its own filtered CTE with its own row count metric,
and queries an active scope only when a check that still runs routes an aggregation metric to it. Core only
calls the hook on a kind that supports scopes.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.common.filtered_cte import build_filtered_cte
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckCollectionImplExtension,
    ContractImpl,
)

SCOPE_ACTIVATION_EXTENSION_NAME: str = "scope_activation_test_extension"


class ScopeActivationExtension(CheckCollectionImplExtension):
    runs_scopes: bool = True
    scope_keys: tuple[str, ...] = ()
    activated_impls: list[CheckCollectionImpl] = []

    def __init__(self, contract_impl: CheckCollectionImpl):
        self.contract_impl: CheckCollectionImpl = contract_impl

    def activate_scopes(self, contract_impl: CheckCollectionImpl) -> None:
        self.activated_impls.append(contract_impl)
        for key in self.scope_keys:
            scope = contract_impl.scopes[key]
            scope.activate(
                cte=build_filtered_cte(
                    contract_impl.dataset_identifier,
                    scope.filter,
                    scope.cte_alias(),
                    contract_impl.filtered_cte_sampler,
                ),
                row_count_metric=contract_impl.metrics_resolver.resolve_metric(
                    RowCountMetricImpl(contract_impl=contract_impl, scope=scope)
                ),
            )

    def build_queries(self, contract_impl: CheckCollectionImpl) -> list:
        queries: list = []
        for scope in contract_impl.scopes.values():
            if not scope.is_active:
                continue
            scope_metrics: list[AggregationMetricImpl] = [
                metric
                for metric in contract_impl.metrics
                if isinstance(metric, AggregationMetricImpl)
                and metric.scope is scope
                and metric.dataset_identifier is contract_impl.dataset_identifier
                and metric.data_source_impl == contract_impl.data_source_impl
            ]
            # A scope whose only metric is its own row count builds no SQL.
            if not any(
                not check_impl.skip and any(metric is scope_metric for scope_metric in scope_metrics)
                for check_impl in contract_impl.all_check_impls
                for metric in check_impl.metrics
            ):
                continue
            queries.extend(contract_impl.bundle_aggregation_metrics(scope_metrics, scope))
        return queries


@contextmanager
def scope_activation(
    *scope_keys: str, impl_class: type[CheckCollectionImpl] = ContractImpl
) -> Iterator[list[CheckCollectionImpl]]:
    """Registers an extension on ``impl_class`` that activates ``scope_keys``, and removes it on exit.

    Yields the list of collections whose scopes the extension was asked to activate.
    """
    activated_impls: list[CheckCollectionImpl] = []
    extension_class = type(
        "ScopeActivationTestExtension",
        (ScopeActivationExtension,),
        {"scope_keys": scope_keys, "activated_impls": activated_impls},
    )
    impl_class.register_extension(SCOPE_ACTIVATION_EXTENSION_NAME, extension_class)
    try:
        yield activated_impls
    finally:
        impl_class.impl_extensions.pop(SCOPE_ACTIVATION_EXTENSION_NAME, None)
