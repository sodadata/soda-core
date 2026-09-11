from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from soda_core.common.data_source_results import QueryResult
from soda_core.contracts.contract_verification import Measurement
from soda_core.contracts.impl.check_types.metric_check import MetricQuery, MetricQueryMetricImpl


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
    # MetricQuery hands the raw cell to the metric's convert_db_value, as the aggregation path does.
    metric_impl.convert_db_value.side_effect = lambda value: value

    metric_query = MetricQuery(
        data_source_impl=data_source_impl,
        metrics=[metric_impl],
        sql="SELECT AVG(value) FROM test_table",
    )

    measurements = metric_query.execute()

    assert len(measurements) == 1
    assert measurements[0].value == pytest.approx(42.0)
    assert measurements[0].metric_id == "test_metric_id"


@pytest.mark.parametrize(
    "db_value, expected",
    [
        pytest.param(None, None, id="null"),
        pytest.param(42, 42, id="int"),
        pytest.param(Decimal("1.50"), Decimal("1.50"), id="decimal_kept"),
        pytest.param(True, True, id="bool_kept"),
        pytest.param("0", 0.0, id="numeric_text"),
        pytest.param(" 12.5 ", 12.5, id="padded_numeric_text"),
        pytest.param("not_a_number", None, id="text"),
        pytest.param("nan", None, id="nan_text"),
        pytest.param("inf", None, id="inf_text"),
        pytest.param(datetime(2026, 8, 17, 5, 9, 32), None, id="datetime"),
        pytest.param(float("nan"), None, id="nan"),
        pytest.param(float("-inf"), None, id="inf"),
        pytest.param(Decimal("NaN"), None, id="decimal_nan"),
        pytest.param(Decimal("sNaN"), None, id="decimal_signalling_nan"),
    ],
)
def test_metric_query_value_is_read_as_a_number(db_value, expected):
    """The metric query value reaches Soda Cloud as a double, so only numbers may pass."""
    metric_impl = object.__new__(MetricQueryMetricImpl)
    metric_impl.query = "SELECT x FROM t"

    value = metric_impl.convert_db_value(db_value)

    assert value == expected
    assert type(value) is type(expected)
