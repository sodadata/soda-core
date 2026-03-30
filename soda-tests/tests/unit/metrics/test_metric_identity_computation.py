"""
Unit tests for metric identity hash computation.

Tests that ConsistentHashBuilder correctly computes consistent and
distinct hashes for different metric identity inputs.
"""

from datetime import date, datetime

from soda_core.common.consistent_hash_builder import ConsistentHashBuilder


def test_consistent_hash_same_inputs_same_hash():
    """Test that same inputs produce the same hash."""
    builder1 = ConsistentHashBuilder()
    builder1.add("metric_type").add("row_count").add("dataset").add("my_data")

    builder2 = ConsistentHashBuilder()
    builder2.add("metric_type").add("row_count").add("dataset").add("my_data")

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_different_inputs_different_hash():
    """Test that different inputs produce different hashes."""
    builder1 = ConsistentHashBuilder()
    builder1.add("metric_type").add("row_count")

    builder2 = ConsistentHashBuilder()
    builder2.add("metric_type").add("missing_count")

    assert builder1.get_hash() != builder2.get_hash()


def test_consistent_hash_order_matters():
    """Test that order of inputs matters for hash computation."""
    builder1 = ConsistentHashBuilder()
    builder1.add("first").add("second")

    builder2 = ConsistentHashBuilder()
    builder2.add("second").add("first")

    assert builder1.get_hash() != builder2.get_hash()


def test_consistent_hash_string_values():
    """Test hash computation with string values."""
    builder = ConsistentHashBuilder()
    builder.add("test_string")

    hash_value = builder.get_hash()
    assert hash_value is not None
    assert isinstance(hash_value, str)
    assert len(hash_value) == 8  # Default hash_string_length


def test_consistent_hash_numeric_values():
    """Test hash computation with numeric values."""
    builder1 = ConsistentHashBuilder()
    builder1.add(42)

    builder2 = ConsistentHashBuilder()
    builder2.add(42)

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_different_numeric_types():
    """Test that int and float produce different hashes due to str() representation."""
    builder_int = ConsistentHashBuilder()
    builder_int.add(42)

    builder_float = ConsistentHashBuilder()
    builder_float.add(42.0)

    # str(42) == "42" but str(42.0) == "42.0", so hashes differ
    assert builder_int.get_hash() != builder_float.get_hash()

    # But same float values produce same hash
    builder_float2 = ConsistentHashBuilder()
    builder_float2.add(42.0)
    assert builder_float.get_hash() == builder_float2.get_hash()


def test_consistent_hash_boolean_values():
    """Test hash computation with boolean values."""
    builder1 = ConsistentHashBuilder()
    builder1.add(True)

    builder2 = ConsistentHashBuilder()
    builder2.add(True)

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_none_values():
    """Test that None values don't affect hash (are skipped)."""
    builder1 = ConsistentHashBuilder()
    builder1.add("test").add(None).add("value")

    builder2 = ConsistentHashBuilder()
    builder2.add("test").add("value")

    # None should be skipped, so these should match
    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_dict_values():
    """Test hash computation with dictionary values uses add_property for each key-value pair."""
    builder1 = ConsistentHashBuilder()
    builder1.add({"key": "value", "count": 42})

    builder2 = ConsistentHashBuilder()
    builder2.add({"key": "value", "count": 42})

    # Same dict with same insertion order produces same hash
    assert builder1.get_hash() == builder2.get_hash()

    # Dict add is equivalent to calling add_property for each item
    builder3 = ConsistentHashBuilder()
    builder3.add_property("key", "value")
    builder3.add_property("count", 42)

    assert builder1.get_hash() == builder3.get_hash()

    # Different dict produces different hash
    builder4 = ConsistentHashBuilder()
    builder4.add({"key": "other", "count": 99})

    assert builder1.get_hash() != builder4.get_hash()


def test_consistent_hash_list_values():
    """Test hash computation with list values."""
    builder1 = ConsistentHashBuilder()
    builder1.add(["metric", "row_count", "dataset"])

    builder2 = ConsistentHashBuilder()
    builder2.add(["metric", "row_count", "dataset"])

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_different_list_order():
    """Test that different list order produces different hash."""
    builder1 = ConsistentHashBuilder()
    builder1.add(["a", "b", "c"])

    builder2 = ConsistentHashBuilder()
    builder2.add(["c", "b", "a"])

    assert builder1.get_hash() != builder2.get_hash()


def test_consistent_hash_date_values():
    """Test hash computation with date values."""
    test_date = date(2024, 3, 11)

    builder1 = ConsistentHashBuilder()
    builder1.add(test_date)

    builder2 = ConsistentHashBuilder()
    builder2.add(test_date)

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_datetime_values():
    """Test hash computation with datetime values."""
    test_datetime = datetime(2024, 3, 11, 10, 30, 45)

    builder1 = ConsistentHashBuilder()
    builder1.add(test_datetime)

    builder2 = ConsistentHashBuilder()
    builder2.add(test_datetime)

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_custom_length():
    """Test hash computation with custom hash_string_length."""
    builder = ConsistentHashBuilder(hash_string_length=16)
    hash_value = builder.add("test").get_hash()

    assert len(hash_value) == 16


def test_consistent_hash_add_property_method():
    """Test add_property method for key-value pairs."""
    builder1 = ConsistentHashBuilder()
    builder1.add_property("metric_type", "row_count")
    builder1.add_property("dataset", "my_data")

    builder2 = ConsistentHashBuilder()
    builder2.add_property("metric_type", "row_count")
    builder2.add_property("dataset", "my_data")

    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_add_property_none_value():
    """Test that None values in properties are skipped."""
    builder1 = ConsistentHashBuilder()
    builder1.add_property("key1", "value1")
    builder1.add_property("key2", None)
    builder1.add_property("key3", "value3")

    builder2 = ConsistentHashBuilder()
    builder2.add_property("key1", "value1")
    builder2.add_property("key3", "value3")

    # None values should be skipped
    assert builder1.get_hash() == builder2.get_hash()


def test_consistent_hash_fluent_interface():
    """Test that ConsistentHashBuilder supports method chaining."""
    builder = ConsistentHashBuilder().add("metric").add("row_count").add("dataset").add("my_data")

    hash_value = builder.get_hash()
    assert hash_value is not None


def test_consistent_hash_empty_builder():
    """Test that empty builder returns None."""
    builder = ConsistentHashBuilder()
    hash_value = builder.get_hash()

    assert hash_value is None


def test_consistent_hash_metric_identity_scenario():
    """Test hash for realistic metric identity scenario."""
    # Simulate metric identity: data_source + dataset + column + metric_type

    # Metric 1: row_count on dataset
    builder1 = ConsistentHashBuilder()
    builder1.add("postgres").add("public").add("users").add("row_count")

    # Same metric
    builder2 = ConsistentHashBuilder()
    builder2.add("postgres").add("public").add("users").add("row_count")

    # Different metric
    builder3 = ConsistentHashBuilder()
    builder3.add("postgres").add("public").add("users").add("missing_count")

    assert builder1.get_hash() == builder2.get_hash()
    assert builder1.get_hash() != builder3.get_hash()


# --- Extra identity properties (extensibility via extra_identity_properties in _build_identity) ---


def test_add_property_none_is_noop():
    """add_property(key, None) must not change the hash.

    This is the foundation of backward compatibility for extensions that add
    optional identity dimensions: when the extra property is absent (None),
    the hash must be identical to one computed without the property at all.
    """
    builder_without = ConsistentHashBuilder(8)
    builder_without.add_property("dso", "ds1")
    builder_without.add_property("pr", "schema")
    builder_without.add_property("ds", "table")
    builder_without.add_property("c", None)
    builder_without.add_property("t", "row_count")
    builder_without.add_property("q", None)
    hash_without = builder_without.get_hash()

    builder_with_none = ConsistentHashBuilder(8)
    builder_with_none.add_property("dso", "ds1")
    builder_with_none.add_property("pr", "schema")
    builder_with_none.add_property("ds", "table")
    builder_with_none.add_property("c", None)
    builder_with_none.add_property("t", "row_count")
    builder_with_none.add_property("q", None)
    builder_with_none.add_property("extra", None)
    hash_with_none = builder_with_none.get_hash()

    assert hash_without == hash_with_none


def test_extra_property_changes_hash():
    """add_property(key, value) with a non-None value must change the hash."""
    builder_without = ConsistentHashBuilder(8)
    builder_without.add_property("dso", "ds1")
    builder_without.add_property("t", "row_count")

    builder_with = ConsistentHashBuilder(8)
    builder_with.add_property("dso", "ds1")
    builder_with.add_property("t", "row_count")
    builder_with.add_property("extra", "some_value")

    assert builder_without.get_hash() != builder_with.get_hash()


def test_different_extra_property_values_produce_different_hashes():
    """Different values for the same extra property key must produce different hashes."""
    builder_a = ConsistentHashBuilder(8)
    builder_a.add_property("t", "row_count")
    builder_a.add_property("extra", "value_a")

    builder_b = ConsistentHashBuilder(8)
    builder_b.add_property("t", "row_count")
    builder_b.add_property("extra", "value_b")

    assert builder_a.get_hash() != builder_b.get_hash()


def test_extra_property_hash_is_deterministic():
    """Same base properties + same extra property must always produce the same hash."""

    def build():
        builder = ConsistentHashBuilder(8)
        builder.add_property("dso", "ds1")
        builder.add_property("pr", "schema")
        builder.add_property("ds", "table")
        builder.add_property("t", "row_count")
        builder.add_property("extra", "value")
        return builder.get_hash()

    assert build() == build()


def test_multiple_none_extra_properties_are_noop():
    """Multiple add_property calls with None values must not change the hash."""
    builder_without = ConsistentHashBuilder(8)
    builder_without.add_property("t", "row_count")
    hash_without = builder_without.get_hash()

    builder_with = ConsistentHashBuilder(8)
    builder_with.add_property("t", "row_count")
    builder_with.add_property("extra1", None)
    builder_with.add_property("extra2", None)
    builder_with.add_property("extra3", None)

    assert builder_with.get_hash() == hash_without


def test_extra_properties_order_independent():
    """Extra properties added in different order must produce the same hash.

    _build_identity sorts the extra_identity_properties dict by key before
    adding them to the hash, so callers don't need to worry about dict key
    ordering.
    """
    from soda_core.contracts.impl.contract_verification_impl import CheckImpl

    hash_ab = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
        extra_identity_properties={"alpha": "a", "beta": "b"},
    )
    hash_ba = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
        extra_identity_properties={"beta": "b", "alpha": "a"},
    )
    assert hash_ab == hash_ba


def test_build_identity_extra_none_preserves_hash():
    """Passing extra_identity_properties with None value must not change the identity."""
    from soda_core.contracts.impl.contract_verification_impl import CheckImpl

    hash_without = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )
    hash_with_none = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
        extra_identity_properties={"src": None},
    )
    assert hash_without == hash_with_none


def test_build_identity_extra_value_changes_hash():
    """Passing a non-None extra property must produce a different identity."""
    from soda_core.contracts.impl.contract_verification_impl import CheckImpl

    hash_without = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
    )
    hash_with = CheckImpl._build_identity(
        contract_impl=_stub_contract_impl(),
        column_impl=None,
        check_type="row_count",
        qualifier=None,
        extra_identity_properties={"src": "warehouse"},
    )
    assert hash_without != hash_with


class _StubDataSourceImpl:
    name = "ds1"


class _StubContractImpl:
    data_source_impl = _StubDataSourceImpl()
    dataset_prefix = "schema"
    dataset_name = "table"


def _stub_contract_impl():
    return _StubContractImpl()
