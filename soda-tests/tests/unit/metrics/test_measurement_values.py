"""
Unit tests for MeasurementValues metric value handling.

Tests that MeasurementValues correctly stores measurement values, retrieves them,
and handles derived metric computation.
"""

from soda_core.contracts.contract_verification import Measurement
from soda_core.contracts.impl.contract_verification_impl import (
    DerivedMetricImpl,
    MeasurementValues,
    MetricImpl,
)


class SimpleMockMetric(MetricImpl):
    """Simple mock metric for testing."""

    def __init__(self, metric_id: str, metric_type: str = "mock"):
        self.id = metric_id
        self.type = metric_type

    def sql_condition_expression(self):
        return None


class SimpleMockDerivedMetric(DerivedMetricImpl):
    """Simple mock derived metric for testing."""

    def __init__(self, metric_id: str, dependencies: list[MetricImpl]):
        self.id = metric_id
        self.type = "derived_mock"
        self._dependencies = dependencies

    def sql_condition_expression(self):
        return None

    def get_metric_dependencies(self) -> list[MetricImpl]:
        return self._dependencies

    def compute_derived_value(self, measurement_values: MeasurementValues) -> float:
        """Sum of all dependencies."""
        total = 0
        for dep in self._dependencies:
            val = measurement_values.get_value(dep)
            if val is not None:
                total += val
        return total


def test_measurement_values_get_value():
    """Test that MeasurementValues retrieves stored measurement values."""
    metric1 = SimpleMockMetric("metric_1")
    metric2 = SimpleMockMetric("metric_2")

    measurements = [
        Measurement(metric_id="metric_1", value=100, metric_name="count"),
        Measurement(metric_id="metric_2", value=50, metric_name="sum"),
    ]

    mv = MeasurementValues(measurements)

    assert mv.get_value(metric1) == 100
    assert mv.get_value(metric2) == 50


def test_measurement_values_get_value_nonexistent():
    """Test that MeasurementValues returns None for non-existent metrics."""
    metric1 = SimpleMockMetric("metric_1")
    metric_not_present = SimpleMockMetric("metric_not_present")

    measurements = [
        Measurement(metric_id="metric_1", value=100, metric_name="count"),
    ]

    mv = MeasurementValues(measurements)

    assert mv.get_value(metric1) == 100
    assert mv.get_value(metric_not_present) is None


def test_measurement_values_with_zero_values():
    """Test that MeasurementValues correctly handles zero values."""
    metric1 = SimpleMockMetric("metric_1")
    metric2 = SimpleMockMetric("metric_2")

    measurements = [
        Measurement(metric_id="metric_1", value=0, metric_name="count"),
        Measurement(metric_id="metric_2", value=0.0, metric_name="percentage"),
    ]

    mv = MeasurementValues(measurements)

    assert mv.get_value(metric1) == 0
    assert mv.get_value(metric2) is not None
    assert abs(mv.get_value(metric2)) < 1e-9


def test_measurement_values_with_none_values():
    """Test that MeasurementValues correctly handles None values in measurements."""
    metric1 = SimpleMockMetric("metric_1")
    metric2 = SimpleMockMetric("metric_2")

    measurements = [
        Measurement(metric_id="metric_1", value=None, metric_name="count"),
        Measurement(metric_id="metric_2", value=100, metric_name="sum"),
    ]

    mv = MeasurementValues(measurements)

    assert mv.get_value(metric1) is None
    assert mv.get_value(metric2) == 100


def test_measurement_values_with_string_values():
    """Test that MeasurementValues can store and retrieve string values."""
    metric1 = SimpleMockMetric("metric_1")

    measurements = [
        Measurement(metric_id="metric_1", value="test_value", metric_name="text"),
    ]

    mv = MeasurementValues(measurements)

    assert mv.get_value(metric1) == "test_value"


def test_measurement_values_derive_value_simple():
    """Test derive_value computes simple derived metrics."""
    base_metric1 = SimpleMockMetric("base_1")
    base_metric2 = SimpleMockMetric("base_2")
    derived = SimpleMockDerivedMetric("derived", [base_metric1, base_metric2])

    measurements = [
        Measurement(metric_id="base_1", value=30, metric_name="count"),
        Measurement(metric_id="base_2", value=20, metric_name="count"),
    ]

    mv = MeasurementValues(measurements)
    mv.derive_value(derived)

    # Should compute sum: 30 + 20 = 50
    assert mv.get_value(derived) == 50


def test_measurement_values_derive_value_not_duplicate():
    """Test that derive_value doesn't recompute if value already exists."""
    base_metric = SimpleMockMetric("base")
    derived = SimpleMockDerivedMetric("derived", [base_metric])

    measurements = [
        Measurement(metric_id="base", value=100, metric_name="count"),
        Measurement(metric_id="derived", value=999, metric_name="computed"),
    ]

    mv = MeasurementValues(measurements)
    mv.derive_value(derived)

    # Should keep the pre-existing value
    assert mv.get_value(derived) == 999


def test_measurement_values_multiple_measurements():
    """Test MeasurementValues with multiple measurements."""
    metrics = [SimpleMockMetric(f"metric_{i}") for i in range(5)]
    measurements = [Measurement(metric_id=f"metric_{i}", value=i * 10, metric_name=f"metric_{i}") for i in range(5)]

    mv = MeasurementValues(measurements)

    for i, metric in enumerate(metrics):
        assert mv.get_value(metric) == i * 10


def test_measurement_values_with_float_precision():
    """Test MeasurementValues preserves float precision."""
    metric1 = SimpleMockMetric("metric_1")

    measurements = [
        Measurement(metric_id="metric_1", value=3.14159265359, metric_name="pi"),
    ]

    mv = MeasurementValues(measurements)

    value = mv.get_value(metric1)
    assert isinstance(value, float)
    assert abs(value - 3.14159265359) < 1e-12


def test_measurement_values_derive_with_empty_dependencies():
    """Test derive_value with metric having no dependencies."""
    derived = SimpleMockDerivedMetric("derived", [])

    measurements = []
    mv = MeasurementValues(measurements)
    mv.derive_value(derived)

    # Should compute sum of empty list = 0
    assert mv.get_value(derived) == 0
