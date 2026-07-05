"""Unit tests for the historic-data fetch methods on SodaCloud (OBSL-1007).

Request/response contract ported from v3 ``SodaCloud.get_historic_measurements``
/ ``get_historic_check_results`` (soda_cloud.py:714-770): POST types
``sodaCoreHistoricMeasurements2`` / ``sodaCoreHistoricCheckResults2``,
minScanTime/maxScanTime XOR limit, v3 millisecond-UTC scan-time strings,
and the 500-identities-per-request backend constraint (batch + merge).
"""

from datetime import datetime, timedelta, timezone

import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.common.soda_cloud import (
    HISTORIC_IDENTITIES_MAX_BATCH_SIZE,
    HistoricDateTimeRange,
    SodaCloudException,
    _convert_scan_time_to_str,
)

DATE_TIME_RANGE = HistoricDateTimeRange(
    from_date_time=datetime(2025, 3, 1, 10, 20, 30, 123000),
    to_date_time=datetime(2025, 3, 8, 10, 20, 30, 123000),
)


def _measurement(identity: str, value=42, **extra) -> dict:
    return {"identity": identity, "value": value, "dataTime": "2025-03-02T00:00:00.000+00:00", **extra}


def _check_result(identity: str, measurement_id: str, **extra) -> dict:
    return {"identity": identity, "measurementId": measurement_id, "outcome": "pass", **extra}


# ---------------------------------------------------------------------------
# scan-time serialization: the v3 byte-exact format
# ---------------------------------------------------------------------------


def test_scan_time_str_naive_datetime_is_interpreted_as_utc_with_milliseconds():
    assert _convert_scan_time_to_str(datetime(2025, 3, 1, 10, 20, 30, 123000)) == "2025-03-01T10:20:30.123+00:00"


def test_scan_time_str_aware_datetime_is_converted_to_utc():
    cest = timezone(timedelta(hours=2))
    assert (
        _convert_scan_time_to_str(datetime(2025, 3, 1, 12, 20, 30, 500000, tzinfo=cest))
        == "2025-03-01T10:20:30.500+00:00"
    )


def test_scan_time_str_whole_seconds_keep_millisecond_precision():
    # v3 JsonHelper used isoformat(timespec="milliseconds"): always 3 fractional digits.
    assert _convert_scan_time_to_str(datetime(2025, 3, 1, 0, 0, 0)) == "2025-03-01T00:00:00.000+00:00"


# ---------------------------------------------------------------------------
# get_historic_measurements
# ---------------------------------------------------------------------------


def test_get_historic_measurements_request_shape_with_date_time_range():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1")])

    mock.get_historic_measurements(["m1", "m2"], date_time_range=DATE_TIME_RANGE)

    request = mock.requests[-1].json
    assert request["type"] == "sodaCoreHistoricMeasurements2"
    assert request["metricIdentities"] == ["m1", "m2"]
    assert request["minScanTime"] == "2025-03-01T10:20:30.123+00:00"
    assert request["maxScanTime"] == "2025-03-08T10:20:30.123+00:00"
    assert "limit" not in request


def test_get_historic_measurements_request_shape_with_limit():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1")])

    mock.get_historic_measurements(["m1"], limit=1000)

    request = mock.requests[-1].json
    assert request["type"] == "sodaCoreHistoricMeasurements2"
    assert request["metricIdentities"] == ["m1"]
    assert request["limit"] == 1000
    assert "minScanTime" not in request
    assert "maxScanTime" not in request


def test_get_historic_measurements_groups_by_identity_by_default():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1", value=1), _measurement("m1", value=2)])
    mock.add_historic_measurements("m2", [_measurement("m2", value=3)])

    grouped = mock.get_historic_measurements(["m1", "m2"], date_time_range=DATE_TIME_RANGE)

    assert set(grouped.keys()) == {"m1", "m2"}
    assert [m["value"] for m in grouped["m1"]] == [1, 2]
    assert [m["value"] for m in grouped["m2"]] == [3]


def test_get_historic_measurements_ungrouped_returns_flat_list():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1", value=1)])
    mock.add_historic_measurements("m2", [_measurement("m2", value=2)])

    results = mock.get_historic_measurements(["m1", "m2"], date_time_range=DATE_TIME_RANGE, group_by_identity=False)

    assert isinstance(results, list)
    assert [m["value"] for m in results] == [1, 2]


def test_get_historic_measurements_drops_measurements_without_value_by_default():
    # v3 drop_measurements_without_value=True semantics (soda_cloud.py:730-734)
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [{"identity": "m1"}, _measurement("m1", value=7)])

    grouped = mock.get_historic_measurements(["m1"], date_time_range=DATE_TIME_RANGE)
    assert [m["value"] for m in grouped["m1"]] == [7]

    kept = mock.get_historic_measurements(
        ["m1"], date_time_range=DATE_TIME_RANGE, drop_measurements_without_value=False
    )
    assert len(kept["m1"]) == 2


def test_get_historic_measurements_batches_above_500_identities_and_merges():
    mock = MockSodaCloud()
    identities = [f"m{i}" for i in range(HISTORIC_IDENTITIES_MAX_BATCH_SIZE + 1)]  # 501
    for identity in identities:
        mock.add_historic_measurements(identity, [_measurement(identity)])

    grouped = mock.get_historic_measurements(identities, date_time_range=DATE_TIME_RANGE)

    # Two POSTs: 500 identities + 1 identity. The mock 400s any request above
    # 500 identities, so this also proves no oversized request was sent.
    assert len(mock.requests) == 2
    assert len(mock.requests[0].json["metricIdentities"]) == HISTORIC_IDENTITIES_MAX_BATCH_SIZE
    assert mock.requests[1].json["metricIdentities"] == ["m500"]
    # both batches carry the same query args
    for request in mock.requests:
        assert request.json["minScanTime"] == "2025-03-01T10:20:30.123+00:00"
    # merged: every identity is present in the grouped result
    assert set(grouped.keys()) == set(identities)


def test_get_historic_measurements_requires_date_time_range_xor_limit():
    mock = MockSodaCloud()
    with pytest.raises(SodaCloudException, match="exactly one"):
        mock.get_historic_measurements(["m1"])
    with pytest.raises(SodaCloudException, match="exactly one"):
        mock.get_historic_measurements(["m1"], date_time_range=DATE_TIME_RANGE, limit=10)
    assert mock.requests == []


# ---------------------------------------------------------------------------
# get_historic_check_results
# ---------------------------------------------------------------------------


def test_get_historic_check_results_request_shape():
    mock = MockSodaCloud()
    mock.add_historic_check_results("c1", [_check_result("c1", "meas-1")])

    mock.get_historic_check_results(["c1", "c2"], date_time_range=DATE_TIME_RANGE)

    request = mock.requests[-1].json
    assert request["type"] == "sodaCoreHistoricCheckResults2"
    assert request["checkIdentities"] == ["c1", "c2"]
    assert request["minScanTime"] == "2025-03-01T10:20:30.123+00:00"
    assert request["maxScanTime"] == "2025-03-08T10:20:30.123+00:00"


def test_get_historic_check_results_groups_by_measurement_id_by_default():
    mock = MockSodaCloud()
    mock.add_historic_check_results(
        "c1", [_check_result("c1", "meas-1"), _check_result("c1", "meas-2", outcome="fail")]
    )
    mock.add_historic_check_results("c2", [_check_result("c2", "meas-1")])

    grouped = mock.get_historic_check_results(["c1", "c2"], date_time_range=DATE_TIME_RANGE)

    assert set(grouped.keys()) == {"meas-1", "meas-2"}
    assert [r["identity"] for r in grouped["meas-1"]] == ["c1", "c2"]
    assert [r["identity"] for r in grouped["meas-2"]] == ["c1"]


def test_get_historic_check_results_ungrouped_returns_flat_list():
    mock = MockSodaCloud()
    mock.add_historic_check_results("c1", [_check_result("c1", "meas-1")])

    results = mock.get_historic_check_results(["c1"], date_time_range=DATE_TIME_RANGE, group_by_measurement_id=False)

    assert isinstance(results, list)
    assert [r["identity"] for r in results] == ["c1"]


def test_get_historic_check_results_batches_above_500_identities_and_merges():
    mock = MockSodaCloud()
    identities = [f"c{i}" for i in range(HISTORIC_IDENTITIES_MAX_BATCH_SIZE + 2)]  # 502
    for index, identity in enumerate(identities):
        mock.add_historic_check_results(identity, [_check_result(identity, f"meas-{index}")])

    grouped = mock.get_historic_check_results(identities, limit=100)

    assert len(mock.requests) == 2
    assert len(mock.requests[0].json["checkIdentities"]) == HISTORIC_IDENTITIES_MAX_BATCH_SIZE
    assert mock.requests[1].json["checkIdentities"] == ["c500", "c501"]
    assert len(grouped) == len(identities)


def test_get_historic_check_results_requires_date_time_range_xor_limit():
    mock = MockSodaCloud()
    with pytest.raises(SodaCloudException, match="exactly one"):
        mock.get_historic_check_results(["c1"])
    with pytest.raises(SodaCloudException, match="exactly one"):
        mock.get_historic_check_results(["c1"], date_time_range=DATE_TIME_RANGE, limit=10)


# ---------------------------------------------------------------------------
# error handling
# ---------------------------------------------------------------------------


def test_historic_query_non_200_raises_soda_cloud_exception():
    from helpers.mock_soda_cloud import MockResponse

    mock = MockSodaCloud(
        responses=[MockResponse(status_code=500, json_object={"message": "boom"})],
    )

    with pytest.raises(SodaCloudException, match="boom"):
        mock.get_historic_measurements(["m1"], date_time_range=DATE_TIME_RANGE)


# ---------------------------------------------------------------------------
# get_last_measurement (OBSL-1028) — v3 soda_cloud.py:703-712 verbatim
# semantics: sodaCoreHistoricMeasurements2 with a single identity and
# ``limit: 1``; first-result-or-None.
# ---------------------------------------------------------------------------


def test_get_last_measurement_request_shape():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1", value=7)])

    mock.get_last_measurement("m1")

    request = mock.requests[-1].json
    assert request["type"] == "sodaCoreHistoricMeasurements2"
    assert request["metricIdentities"] == ["m1"]
    assert request["limit"] == 1
    assert "minScanTime" not in request
    assert "maxScanTime" not in request


def test_get_last_measurement_returns_first_result():
    mock = MockSodaCloud()
    mock.add_historic_measurements("m1", [_measurement("m1", value=7), _measurement("m1", value=8)])

    result = mock.get_last_measurement("m1")

    assert result is not None
    assert result["value"] == 7


def test_get_last_measurement_returns_none_when_no_results():
    mock = MockSodaCloud()
    mock.add_historic_measurements("other", [_measurement("other")])

    assert mock.get_last_measurement("m1") is None


def test_get_last_measurement_non_200_raises_soda_cloud_exception():
    from helpers.mock_soda_cloud import MockResponse

    mock = MockSodaCloud(
        responses=[MockResponse(status_code=500, json_object={"message": "boom"})],
    )

    with pytest.raises(SodaCloudException, match="boom"):
        mock.get_last_measurement("m1")
