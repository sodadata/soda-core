"""
Unit tests for MetricsResolver deduplication logic.

Tests that MetricsResolver correctly deduplicates metrics based on identity
and returns the same metric instance for equivalent metric configurations.
"""

from soda_core.contracts.impl.contract_verification_impl import (
    MetricImpl,
    MetricsResolver,
)


class MockMetricImpl(MetricImpl):
    """Mock metric implementation for testing.

    Mirrors production MetricImpl equality semantics:
    - __eq__ uses exact type match (type(other) != type(self)) and compares self.id
    - id is a string built from metric_type and identifier, simulating the hash-based
      ID that production MetricImpl builds via ConsistentHashBuilder._build_id()
    """

    def __init__(self, metric_type: str, identifier: str):
        # Note: This is a simplified mock. Real MetricImpl requires contract_impl
        self.metric_type = metric_type
        self.identifier = identifier
        self.id = f"{metric_type}_{identifier}"
        self.type = metric_type

    def __eq__(self, other):
        """Mirrors production MetricImpl: exact type check + id comparison."""
        if type(other) != type(self):
            return False
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)


def test_metrics_resolver_resolves_new_metric():
    """Test that MetricsResolver adds new metrics to its list."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("row_count", "dataset_1")
    resolved1 = resolver.resolve_metric(metric1)

    assert resolved1 is metric1
    assert len(resolver.get_resolved_metrics()) == 1


def test_metrics_resolver_deduplicates_identical_metrics():
    """Test that MetricsResolver deduplicates identical metrics."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("missing_count", "column_id")
    metric2 = MockMetricImpl("missing_count", "column_id")  # Same type and identifier

    resolved1 = resolver.resolve_metric(metric1)
    resolved2 = resolver.resolve_metric(metric2)

    # Should return the same instance
    assert resolved1 is metric1
    assert resolved2 is metric1  # Returns the first one
    assert len(resolver.get_resolved_metrics()) == 1


def test_metrics_resolver_keeps_different_metrics():
    """Test that MetricsResolver keeps metrics with different identities."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("row_count", "dataset_1")
    metric2 = MockMetricImpl("row_count", "dataset_2")

    resolved1 = resolver.resolve_metric(metric1)
    resolved2 = resolver.resolve_metric(metric2)

    assert resolved1 is metric1
    assert resolved2 is metric2
    assert len(resolver.get_resolved_metrics()) == 2


def test_metrics_resolver_keeps_different_types():
    """Test that MetricsResolver keeps metrics with different types."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("row_count", "dataset_1")
    metric2 = MockMetricImpl("missing_count", "dataset_1")

    resolved1 = resolver.resolve_metric(metric1)
    resolved2 = resolver.resolve_metric(metric2)

    assert resolved1 is metric1
    assert resolved2 is metric2
    assert len(resolver.get_resolved_metrics()) == 2


def test_metrics_resolver_multiple_deduplication():
    """Test deduplication with multiple operations."""
    resolver = MetricsResolver()

    metrics = [
        MockMetricImpl("row_count", "ds1"),
        MockMetricImpl("row_count", "ds2"),
        MockMetricImpl("row_count", "ds1"),  # Duplicate
        MockMetricImpl("missing", "col1"),
        MockMetricImpl("missing", "col1"),  # Duplicate
        MockMetricImpl("missing", "col2"),
        MockMetricImpl("row_count", "ds1"),  # Another duplicate
    ]

    resolved = [resolver.resolve_metric(m) for m in metrics]

    # Should have 4 unique metrics
    unique_metrics = resolver.get_resolved_metrics()
    assert len(unique_metrics) == 4

    # Check the ones that were duplicated point to the same instance
    assert resolved[0] is resolved[2]  # row_count ds1
    assert resolved[0] is resolved[6]  # row_count ds1 again
    assert resolved[3] is resolved[4]  # missing col1


def test_metrics_resolver_order_preservation():
    """Test that MetricsResolver preserves order of unique metrics."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("type1", "id1")
    metric2 = MockMetricImpl("type2", "id2")
    metric3 = MockMetricImpl("type3", "id3")

    resolver.resolve_metric(metric1)
    resolver.resolve_metric(metric2)
    resolver.resolve_metric(metric3)

    metrics = resolver.get_resolved_metrics()
    assert metrics[0] is metric1
    assert metrics[1] is metric2
    assert metrics[2] is metric3


def test_metrics_resolver_empty_initially():
    """Test that MetricsResolver starts empty."""
    resolver = MetricsResolver()
    assert len(resolver.get_resolved_metrics()) == 0


def test_metrics_resolver_returns_same_instance_on_second_call():
    """Test that resolver always returns the first instance for duplicates."""
    resolver = MetricsResolver()

    metric1 = MockMetricImpl("test_metric", "id_1")
    metric2 = MockMetricImpl("test_metric", "id_1")

    resolved1 = resolver.resolve_metric(metric1)
    resolved2 = resolver.resolve_metric(metric2)

    # Should return the exact same instance (first one added)
    assert resolved1 is metric1
    assert resolved2 is metric1
    assert id(resolved1) == id(resolved2)
