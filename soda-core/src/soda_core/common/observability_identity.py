"""Stable observability identity helpers.

Metric and check identities are the join keys between locally computed
measurements and the historic data Soda Cloud stores for them, so they must
stay byte-identical across releases and across features (profiling, metric
monitoring). That cross-feature stability is why they live in soda-core
rather than in a feature plugin.

The ``ConsistentHashBuilder.add`` order in ``get_metric_identity`` is
load-bearing: data_source, table, partition_name, column (only if not None),
then metric_name, dqn (only if not None). Skipping ``None`` equals not adding
at all. Digests are pinned in
``soda-tests/tests/unit/test_observability_identity.py``.
"""

from __future__ import annotations

from typing import Optional

from soda_core.common.consistent_hash_builder import ConsistentHashBuilder


def get_metric_identity(
    data_source_name: str,
    table_name: str,
    partition_name: str,
    metric_name: str,
    column_name: Optional[str] = None,
    dataset_qualified_name: Optional[str] = None,
) -> str:
    """Library-generated metric identity (8-hex blake2b digest).

    Only the fallback path: a BE-provided ``metricIdentity`` always wins.
    """
    hash_builder = ConsistentHashBuilder()

    hash_builder.add(data_source_name)
    hash_builder.add(table_name)
    hash_builder.add(partition_name)
    if column_name is not None:
        hash_builder.add(column_name)
    hash_builder.add(metric_name)
    if dataset_qualified_name is not None:
        hash_builder.add(dataset_qualified_name)

    return hash_builder.get_hash()


def get_check_identity(metric_identity: str) -> str:
    """Identity of the (metric monitoring) check derived from a metric identity."""
    return f"{metric_identity}-metric-monitoring"
