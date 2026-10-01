"""
Unit tests for the timestamp conversions in LenientTypeConverter.

Athena renders query results as strings; pyathena converts them per result-column
type name. Two gaps bit real scans:
- 'timestamp with time zone' (e.g. Iceberg timestamptz columns) is only mapped by
  pyathena >= 3.9.0 — older versions hand the raw string ('... UTC') to the engine.
- pyathena's own tz converter requires fractional seconds and silently drops
  offset-style zones ('+02:00'), yielding naive datetimes.
LenientTypeConverter must produce correct (aware) datetimes for every form Athena
renders, on any supported pyathena.
"""

from datetime import datetime, timedelta, timezone

import pytest
from soda_athena.common.data_sources.athena_data_source_connection import LenientTypeConverter


@pytest.fixture
def converter() -> LenientTypeConverter:
    return LenientTypeConverter()


def test_timestamp_with_fraction(converter):
    assert converter.convert("timestamp", "2026-08-31 06:00:00.123") == datetime(2026, 8, 31, 6, 0, 0, 123000)


def test_timestamp_without_fraction(converter):
    assert converter.convert("timestamp", "2026-08-31 06:00:00") == datetime(2026, 8, 31, 6, 0, 0)


def test_timestamp_nanosecond_fraction_is_truncated_to_microseconds(converter):
    assert converter.convert("timestamp", "2026-08-31 06:00:00.123456789") == datetime(2026, 8, 31, 6, 0, 0, 123456)


def test_timestamp_none(converter):
    assert converter.convert("timestamp", None) is None


def test_timestamp_tz_named_utc(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00.123 UTC")
    assert value == datetime(2026, 8, 31, 6, 0, 0, 123000, tzinfo=timezone.utc)
    assert value.utcoffset() == timedelta(0)


def test_timestamp_tz_without_fraction(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00 UTC")
    assert value == datetime(2026, 8, 31, 6, 0, 0, tzinfo=timezone.utc)


def test_timestamp_tz_nanosecond_fraction(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00.123456789 UTC")
    assert value == datetime(2026, 8, 31, 6, 0, 0, 123456, tzinfo=timezone.utc)


def test_timestamp_tz_positive_offset_is_not_dropped(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00.000 +02:00")
    assert value.utcoffset() == timedelta(hours=2)
    assert value == datetime(2026, 8, 31, 4, 0, 0, tzinfo=timezone.utc)


def test_timestamp_tz_negative_offset(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00.000 -05:30")
    assert value.utcoffset() == -timedelta(hours=5, minutes=30)


def test_timestamp_tz_region_zone(converter):
    value = converter.convert("timestamp with time zone", "2026-08-31 06:00:00.000 Europe/Zurich")
    # CEST in August
    assert value.utcoffset() == timedelta(hours=2)


def test_timestamp_tz_none(converter):
    assert converter.convert("timestamp with time zone", None) is None


def test_timestamp_tz_unknown_zone_raises_with_the_value(converter):
    with pytest.raises(ValueError, match=r"No/Such_Zone.*2026-08-31 06:00:00\.000 No/Such_Zone"):
        converter.convert("timestamp with time zone", "2026-08-31 06:00:00.000 No/Such_Zone")
