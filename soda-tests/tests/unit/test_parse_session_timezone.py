"""Unit tests for ``parse_session_timezone``.

Covers each branch of the parser explicitly, including the edge cases that the
single-line ``connection.get_session_timezone()`` fallback layer relies on. These
tests are intentionally kept independent of any live datasource — the integration
test ``test_session_timezone.py`` exercises the SQL round-trip through real
connections; here we lock down the pure-function behavior so future changes to
the parser cannot silently regress.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from soda_core.common.data_source_connection import parse_session_timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover — Python <3.9
    ZoneInfo = None


class TestNamedZones:
    """IANA-zone names round-trip into a real ``ZoneInfo`` so DST is honored downstream."""

    @pytest.mark.skipif(ZoneInfo is None, reason="ZoneInfo unavailable on this Python")
    def test_iana_name_returns_zoneinfo(self) -> None:
        result = parse_session_timezone("America/Los_Angeles")
        assert isinstance(result, ZoneInfo)
        assert str(result) == "America/Los_Angeles"

    # Note: ``Etc/UTC`` and other UTC-equivalent IANA aliases are intentionally
    # collapsed to ``timezone.utc`` (the singleton) — see
    # ``TestUtcAliasIanaZoneNormalization`` below. The previous expectation that
    # ``parse_session_timezone("Etc/UTC")`` returned a ``ZoneInfo`` instance was a
    # pre-Copilot-review behavior; the docstring's identity-parity claim made it
    # incorrect.


class TestUtcShorthands:
    """``UTC`` / ``GMT`` / ``Z`` and any zero-offset literal collapse to ``timezone.utc``
    by *identity*, so downstream ``is timezone.utc`` checks behave consistently across
    adapters that report either an IANA name or a numeric offset."""

    @pytest.mark.parametrize("value", ["UTC", "GMT", "Z", "utc", "gmt", "z", " UTC ", "'UTC'"])
    def test_utc_shorthand_returns_utc_identity(self, value: str) -> None:
        assert parse_session_timezone(value) is timezone.utc

    @pytest.mark.parametrize("value", ["+00:00", "-00:00", "+0000", "-0000", "UTC+00:00", "GMT-00:00"])
    def test_zero_offset_normalizes_to_utc_identity(self, value: str) -> None:
        # Without normalization a driver reporting "+00:00" would yield
        # ``timezone(timedelta(0))`` — equal to ``timezone.utc`` but not the same
        # instance. Adapters routed through this parser produce a uniform UTC.
        assert parse_session_timezone(value) is timezone.utc


class TestNumericOffsets:
    """Non-zero numeric offsets are returned as ``datetime.timezone(timedelta(...))``."""

    @pytest.mark.parametrize(
        "value, expected_offset",
        [
            ("+05:30", timedelta(hours=5, minutes=30)),
            ("-08:00", -timedelta(hours=8)),
            ("+0530", timedelta(hours=5, minutes=30)),
            ("-0800", -timedelta(hours=8)),
            ("UTC+02:00", timedelta(hours=2)),
            ("GMT-05:00", -timedelta(hours=5)),
            ("+1", timedelta(hours=1)),
            ("-1", -timedelta(hours=1)),
            # Single-digit offsets without a colon are valid Postgres ``SHOW timezone``
            # outputs when the server is configured as ``timezone = '-7'`` or similar.
            # Pin the parser's acceptance so a regex tightening doesn't silently break
            # those Postgres deployments.
            ("-7", -timedelta(hours=7)),
            ("+8", timedelta(hours=8)),
        ],
    )
    def test_offset_string_returns_fixed_offset(self, value: str, expected_offset: timedelta) -> None:
        result = parse_session_timezone(value)
        # ``utcoffset`` requires a (possibly None) datetime arg for tzinfo subclasses.
        assert result.utcoffset(datetime(2024, 1, 1)) == expected_offset

    def test_offset_string_strips_quotes(self) -> None:
        assert parse_session_timezone("'+05:30'") == timezone(timedelta(hours=5, minutes=30))


class TestEmptyInputFallback:
    """Empty / None / whitespace-only input is the conventional adapter signal for
    'no session TZ configured' (every ``_fetch_session_timezone`` returns
    ``row[0] if row else ""`` on a missing-row result). Returning UTC silently here
    keeps cold-start logs clean; the colleague-review concern (A2) was specifically
    about *non-empty* junk, covered by ``TestUnparseableInputRaises``.
    """

    def _assert_silent_utc_fallback(self, value, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            assert parse_session_timezone(value) is timezone.utc
        assert caplog.records == []

    @pytest.mark.parametrize("value", ["", "   ", "\t\n", None])
    def test_empty_or_none_returns_utc_silently(self, value, caplog: pytest.LogCaptureFixture) -> None:
        # No noise on the conventional empty-fallback path.
        self._assert_silent_utc_fallback(value, caplog)

    @pytest.mark.parametrize("value", ["''", '""', "' '", '"  "'])
    def test_quoted_empty_returns_utc_silently(self, value, caplog: pytest.LogCaptureFixture) -> None:
        # Some drivers quote their session-TZ result (e.g. Postgres ``SHOW`` variants
        # under particular configs return ``''``). After stripping the outer quotes the
        # body is empty — this is the same conceptual fallback as a raw empty input and
        # must not produce a warning.
        self._assert_silent_utc_fallback(value, caplog)


class TestUnparseableInputRaises:
    """Non-empty unparseable input must raise ``ValueError`` so the single fallback layer
    in ``DataSourceConnection.get_session_timezone()`` can catch and log uniformly. The
    pre-fix behavior degraded silently to UTC with only a debug-level log, masking broken
    adapter implementations that yielded garbage values (the colleague-review A2 concern).
    """

    @pytest.mark.parametrize(
        "value",
        [
            "junk",
            "Mars/Olympus",
            "+99:99",
            "five o'clock",
            # Out-of-range minute / hour components that the regex allows. Without an
            # explicit range check, ``+05:99`` would silently normalize to ``+06:39``
            # via timedelta arithmetic, misinterpreting junk as a real but wrong
            # offset (Copilot review #1).
            "+05:99",
            "-05:99",
            "+24:00",
            "+00:60",
        ],
    )
    def test_unparseable_input_raises(self, value: str) -> None:
        with pytest.raises(ValueError, match="could not parse"):
            parse_session_timezone(value)


class TestUtcAliasIanaZoneNormalization:
    """``Etc/UTC`` and friends collapse to ``timezone.utc`` (the singleton) so an
    adapter reporting an IANA-aliased UTC zone matches an adapter reporting the
    literal ``UTC`` string or a ``+00:00`` numeric offset (Copilot review #2).
    """

    @pytest.mark.parametrize(
        "value",
        ["Etc/UTC", "Universal", "Zulu", "Etc/Zulu"],
    )
    def test_iana_alias_for_utc_returns_timezone_utc_identity(self, value: str) -> None:
        result = parse_session_timezone(value)
        assert result is timezone.utc, (
            f"Expected {value!r} to normalize to timezone.utc identity (so adapters "
            f"that report this alias agree with the literal-UTC path), got {result!r}"
        )

    def test_real_iana_zone_returns_zoneinfo_not_utc(self) -> None:
        # Sanity check: a non-UTC-aliased zone still returns a ZoneInfo, not collapsed.
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            pytest.skip("ZoneInfo unavailable on this Python")
        result = parse_session_timezone("America/Los_Angeles")
        assert isinstance(result, ZoneInfo)
        assert result is not timezone.utc

    @pytest.mark.parametrize(
        "value",
        [
            "Etc/GMT+0",
            "Etc/GMT-0",
            "Etc/GMT0",
            "Etc/GMT",
            "Etc/Universal",
            "Etc/Greenwich",
            "GMT0",
            "GMT+0",
            "GMT-0",
            "Greenwich",
        ],
    )
    def test_extended_utc_aliases_collapse_to_timezone_utc(self, value: str) -> None:
        # The ``Etc/GMT+0`` / ``Etc/GMT-0`` family are zero-offset zones in the IANA
        # database. DuckDB's ``current_setting('TimeZone')`` can return ``GMT0``;
        # Postgres can return ``Greenwich``. The first-pass whitelist missed these.
        assert parse_session_timezone(value) is timezone.utc


class TestHostLocalLiteralValues:
    """Postgres ``timezone = 'localtime'`` and Trino's ``current_timezone() = 'system'``
    both mean "use the host's local TZ". The parser previously rejected these as
    unparseable, the connection wrapper caught and returned UTC, and naive returns
    from TZ-aware columns silently shifted by the host's offset. Now resolves to the
    OS-local TZ.
    """

    def test_localtime_resolves_to_host_local_tzinfo(self) -> None:
        result = parse_session_timezone("localtime")
        # The result must be a tzinfo whose offset matches the host's current offset.
        # Don't pin a specific zone (the test runs on multiple hosts).
        from datetime import datetime as _dt

        host_local = _dt.now().astimezone().tzinfo
        assert result.utcoffset(_dt.now()) == host_local.utcoffset(_dt.now())

    def test_system_resolves_to_host_local_tzinfo(self) -> None:
        result = parse_session_timezone("system")
        from datetime import datetime as _dt

        host_local = _dt.now().astimezone().tzinfo
        assert result.utcoffset(_dt.now()) == host_local.utcoffset(_dt.now())

    @pytest.mark.parametrize("value", ["LOCALTIME", "SYSTEM", "LocalTime", "  localtime  ", "'localtime'"])
    def test_localtime_case_and_whitespace_insensitive(self, value: str) -> None:
        # Drivers may upper-case, lower-case, quote, or pad their reported value;
        # all forms should resolve to host-local.
        result = parse_session_timezone(value)
        assert result is not None
        assert hasattr(result, "utcoffset")
