"""Byte-parity pins for soda_core.common.observability_identity (OBSL-1007).

Every pinned digest below was produced by executing the ACTUAL v3
implementation (soda-library, pinned venv) on 2026-07-05:

    /home/mvds/soda/repos/soda-library/.venv/bin/python -c \
        "from soda.profiling.model.profiling_metric import get_metric_identity; ..."

v3 sources: get_metric_identity from
``src/soda/soda/profiling/model/profiling_metric.py:113-132``;
get_check_identity from ``src/observability/soda/observability/config.py:145-147``.
Prod-sourced identities were not obtainable (prod unreachable from the dev
environment and the golden payloads mask identities), so executing the v3 code
itself is the strongest available byte-evidence.

These digests also anchor the metric-monitoring hash-pin tests in
soda-extensions (``soda-metric-monitoring/tests/test_identity.py``: e07cd407,
ae0b66d7), which delegate to this module. Do NOT update any pin here without
a v3-parity investigation.
"""

from soda_core.common.observability_identity import (
    get_check_identity,
    get_metric_identity,
)


def test_dataset_metric_identity_pin():
    """v3: ["postgres_ds","orders","ts_days_1","row_count","postgres_ds/public/orders"] -> e07cd407."""
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
    """v3: same inputs + column cst_size + metric avg -> ae0b66d7.

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
    """v3: ["ds","t","p","m"] -> 48cbdae7 (None column/dqn are not added at all)."""
    assert get_metric_identity("ds", "t", "p", "m") == "48cbdae7"


def test_identity_with_dqn_only_pin():
    """v3: ["ds","t","p","m","ds/t"] -> 01a0fd9d (dqn changes the digest)."""
    assert get_metric_identity("ds", "t", "p", "m", dataset_qualified_name="ds/t") == "01a0fd9d"


def test_identity_unicode_pin():
    """v3: unicode table/column names hash via utf-8 -> 6f3adbbe."""
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
    """v3: ["bq","events","ts_days_7","user_id","missing_count"] -> 372d687c."""
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
    """v3 config.py:145-147 -> '<metric_identity>-metric-monitoring'."""
    assert get_check_identity("e07cd407") == "e07cd407-metric-monitoring"
