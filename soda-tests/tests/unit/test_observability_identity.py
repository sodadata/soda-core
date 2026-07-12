"""Pinned identity digests for soda_core.common.observability_identity.

Metric and check identities are the join keys with the historic data Soda
Cloud stores, so the digests below must never change. They also anchor the
metric-monitoring hash-pin tests in soda-extensions
(``soda-metric-monitoring/tests/test_identity.py``: e07cd407, ae0b66d7),
which delegate to this module. Do NOT update any pin here.
"""

from soda_core.common.observability_identity import (
    get_check_identity,
    get_metric_identity,
)


def test_dataset_metric_identity_pin():
    """["postgres_ds","orders","ts_days_1","row_count","postgres_ds/public/orders"] -> e07cd407."""
    assert (
        get_metric_identity(
            data_source_name="postgres_ds",
            table_name="orders",
            partition_name="ts_days_1",
            metric_name="row_count",
            dataset_qualified_name="postgres_ds/public/orders",
        )
        == "e07cd407"
    )


def test_column_metric_identity_pin():
    """Same inputs + column cst_size + metric avg -> ae0b66d7.

    The add order is data_source, table, partition, COLUMN (only if not None),
    then METRIC, dqn (only if not None) — column-before-metric is load-bearing.
    """
    assert (
        get_metric_identity(
            data_source_name="postgres_ds",
            table_name="orders",
            partition_name="ts_days_1",
            metric_name="avg",
            column_name="cst_size",
            dataset_qualified_name="postgres_ds/public/orders",
        )
        == "ae0b66d7"
    )


def test_identity_without_column_and_dqn_pin():
    """["ds","t","p","m"] -> 48cbdae7 (None column/dqn are not added at all)."""
    assert get_metric_identity("ds", "t", "p", "m") == "48cbdae7"


def test_identity_with_dqn_only_pin():
    """["ds","t","p","m","ds/t"] -> 01a0fd9d (dqn changes the digest)."""
    assert get_metric_identity("ds", "t", "p", "m", dataset_qualified_name="ds/t") == "01a0fd9d"


def test_identity_unicode_pin():
    """Unicode table/column names hash via utf-8 -> 6f3adbbe."""
    assert (
        get_metric_identity(
            data_source_name="snowflake_ds",
            table_name="Ürün_Tablosu",
            partition_name="ts_hours_6",
            metric_name="distinct",
            column_name="katégoría_ñame",
            dataset_qualified_name="snowflake_ds/PUBLIC/Ürün_Tablosu",
        )
        == "6f3adbbe"
    )


def test_identity_with_column_without_dqn_pin():
    """["bq","events","ts_days_7","user_id","missing_count"] -> 372d687c."""
    assert (
        get_metric_identity(
            data_source_name="bq",
            table_name="events",
            partition_name="ts_days_7",
            metric_name="missing_count",
            column_name="user_id",
        )
        == "372d687c"
    )


def test_get_check_identity_pin():
    assert get_check_identity("e07cd407") == "e07cd407-metric-monitoring"
