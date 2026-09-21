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


def test_metric_query_execute_does_not_warn_on_a_scalar_result(caplog):
    data_source_impl = MagicMock()
    data_source_impl.execute_query.return_value = QueryResult(rows=[(42.0,)], columns=(("value",),))

    metric_impl = MagicMock()
    metric_impl.convert_db_value.side_effect = lambda value: value

    metric_query = MetricQuery(
        data_source_impl=data_source_impl, metrics=[metric_impl], sql="SELECT AVG(value) FROM test_table"
    )

    with caplog.at_level("WARNING"):
        metric_query.execute()

    assert "Metric query returned" not in caplog.text


@pytest.mark.parametrize(
    "rows, columns, expected_warning",
    [
        pytest.param(
            [(42.0,), (7.0,), (1.0,)],
            (("value",),),
            "Metric query returned 3 rows, expected 1. Using the first column of the first row. "
            "Which row comes first is nondeterministic without an ORDER BY.",
            id="multi_row",
        ),
        pytest.param(
            [(42.0, 7.0)],
            (("value",), ("other",)),
            "Metric query returned 2 columns, expected 1. Using the first column of the first row.\n",
            id="multi_column",
        ),
        pytest.param(
            [(42.0, 7.0), (1.0, 2.0)],
            (("value",), ("other",)),
            "Metric query returned 2 rows, expected 1 and 2 columns, expected 1. "
            "Using the first column of the first row. "
            "Which row comes first is nondeterministic without an ORDER BY.",
            id="multi_row_and_multi_column",
        ),
    ],
)
def test_metric_query_execute_warns_on_a_non_scalar_result_and_reads_the_first_cell(
    rows, columns, expected_warning, caplog
):
    """A metric query is read as rows[0][0]. Extra rows and columns are dropped, with a warning naming them."""
    data_source_impl = MagicMock()
    data_source_impl.execute_query.return_value = QueryResult(rows=rows, columns=columns)

    metric_impl = MagicMock()
    metric_impl.id = "test_metric_id"
    metric_impl.type = "metric"
    metric_impl.convert_db_value.side_effect = lambda value: value

    sql = "SELECT value, other FROM test_table"
    metric_query = MetricQuery(data_source_impl=data_source_impl, metrics=[metric_impl], sql=sql)

    with caplog.at_level("WARNING"):
        measurements = metric_query.execute()

    assert len(measurements) == 1
    assert measurements[0].value == pytest.approx(42.0)

    warnings = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
    assert len(warnings) == 1
    assert expected_warning in warnings[0]
    assert ("ORDER BY" in warnings[0]) == (len(rows) > 1)
    assert sql in warnings[0]


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
