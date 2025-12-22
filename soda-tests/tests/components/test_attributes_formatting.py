from datetime import date, datetime, timezone

import pytest
from soda_core.common.soda_cloud_dto import CheckAttribute


class TestCheckAttributeFromRaw:
    def test_sets_name_and_formats_value(self):
        ca = CheckAttribute.from_raw("x", 123)
        assert ca.name == "x"
        assert ca.value == "123"


class TestCheckAttributeFormatValueBooleans:
    @pytest.mark.parametrize("v", [True, False])
    def test_bool_passthrough(self, v):
        assert CheckAttribute._format_value(v) == v

    @pytest.mark.parametrize(
        "v, expected",
        [
            ("true", True),
            ("false", False),
            ("TRUE", True),
            ("FALSE", False),
            ("TrUe", True),
            ("FaLsE", False),
        ],
    )
    def test_string_boolean(self, v, expected):
        assert CheckAttribute._format_value(v) == expected

    def test_non_boolean_strings_not_converted(self):
        assert CheckAttribute._format_value("yes") == "yes"
        assert CheckAttribute._format_value("0") == "0"
        assert CheckAttribute._format_value("") == ""


class TestCheckAttributeFormatValueDatesAndDatetimes:
    def test_date_converted_to_datetime_midnight(self):
        d = date(2025, 1, 2)
        out = CheckAttribute._format_value(d)
        # date -> datetime midnight -> isoformat string with tz attached (local tz)
        assert isinstance(out, str)
        assert out.startswith("2025-01-02T00:00:00")

    def test_naive_datetime_gets_local_tz_and_isoformat(self):
        dt = datetime(2025, 1, 2, 3, 4, 5)  # naive
        out = CheckAttribute._format_value(dt)
        assert isinstance(out, str)

        parsed = datetime.fromisoformat(out)
        assert parsed.tzinfo is not None
        assert parsed.year == 2025 and parsed.month == 1 and parsed.day == 2
        assert parsed.hour == 3 and parsed.minute == 4 and parsed.second == 5

    def test_aware_datetime_keeps_existing_tz(self):
        dt = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        out = CheckAttribute._format_value(dt)
        assert out == dt.isoformat()

        parsed = datetime.fromisoformat(out)
        assert parsed.tzinfo == timezone.utc

    def test_datetime_not_reinterpreted_as_date(self):
        dt = datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
        out = CheckAttribute._format_value(dt)
        assert out == dt.isoformat()


class TestCheckAttributeFormatValueNumbers:
    @pytest.mark.parametrize(
        "v, expected",
        [
            (0, "0"),
            (42, "42"),
            (-7, "-7"),
            (3.5, "3.5"),
        ],
    )
    def test_numbers_to_string(self, v, expected):
        assert CheckAttribute._format_value(v) == expected


class TestCheckAttributeFormatValueOtherTypes:
    def test_none_passthrough(self):
        assert CheckAttribute._format_value(None) is None

    def test_list_passthrough(self):
        v = [1, "a", True]
        assert CheckAttribute._format_value(v) == v

    def test_dict_passthrough(self):
        v = {"a": 1}
        assert CheckAttribute._format_value(v) == v

    def test_bytes_passthrough(self):
        v = b"abc"
        assert CheckAttribute._format_value(v) == v
