from unittest.mock import MagicMock

import pytest
from soda_core.common.data_source_results import QueryResult
from soda_core.contracts.contract_verification import Measurement
from soda_core.contracts.impl.check_types.metric_check import MetricQuery


def test_metric_query_execute_returns_empty_when_query_returns_no_rows():
    """When a metric query returns 0 rows, execute() should not crash with IndexError.

    Instead, it should handle the empty result gracefully and return a Measurement
    with value=None, not raise an unhandled exception.
    """
    data_source_impl = MagicMock()
    data_source_impl.execute_query.return_value = QueryResult(rows=[], columns=(("value",),))

    metric_impl = MagicMock()
    metric_impl.id = "test_metric_id"
    metric_impl.type = "metric"

    metric_query = MetricQuery(
        data_source_impl=data_source_impl,
        metrics=[metric_impl],
        sql="SELECT value FROM test_table WHERE 1 = 0",
    )

    measurements = metric_query.execute()

    assert isinstance(measurements, list)
    assert len(measurements) == 1
    measurement = measurements[0]
    assert isinstance(measurement, Measurement)
    assert measurement.value is None
    assert measurement.metric_id == "test_metric_id"


def test_metric_query_execute_returns_value_when_query_returns_rows():
    """Normal case: query returns a row with a value."""
    data_source_impl = MagicMock()
    data_source_impl.execute_query.return_value = QueryResult(rows=[(42.0,)], columns=(("value",),))

    metric_impl = MagicMock()
    metric_impl.id = "test_metric_id"
    metric_impl.type = "metric"

    metric_query = MetricQuery(
        data_source_impl=data_source_impl,
        metrics=[metric_impl],
        sql="SELECT AVG(value) FROM test_table",
    )

    measurements = metric_query.execute()

    assert len(measurements) == 1
    assert measurements[0].value == pytest.approx(42.0)
    assert measurements[0].metric_id == "test_metric_id"
