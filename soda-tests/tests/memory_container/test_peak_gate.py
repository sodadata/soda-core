"""Unit tests for the memory_container plugin's peak regression gate.

The cgroup cap only guards against OOM — a test can grow from 230 MB to
900 MB and still pass under a 1024 MB cap. The gate fails a clean run when
its peak exceeds an explicit expect_peak_mb marker band or the recorded
per-datasource baseline (+tolerance) in memory_baselines.json.
"""

from __future__ import annotations

import json

from helpers.memory_container_plugin import (
    BASELINES_FILENAME,
    _lookup_peak_baseline,
    _peak_gate_failure,
)


class _FakeMarker:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _FakeItem:
    def __init__(self, fspath, name, marker_kwargs=None):
        self.fspath = fspath
        self.name = name
        self.nodeid = f"{fspath}::{name}"
        self._marker = _FakeMarker(**(marker_kwargs if marker_kwargs is not None else {"limit_mb": 1024}))

    def get_closest_marker(self, marker_name):
        return self._marker


def _write_manifest(tmp_path, manifest: dict) -> None:
    (tmp_path / BASELINES_FILENAME).write_text(json.dumps(manifest))


class TestExpectPeakMarkerBand:
    def test_peak_inside_band_passes(self, tmp_path):
        item = _FakeItem(tmp_path / "test_x.py", "test_y", {"limit_mb": 512, "expect_peak_mb": (220, 330)})
        assert _peak_gate_failure(item, 251.3) is None

    def test_peak_above_band_fails(self, tmp_path):
        item = _FakeItem(tmp_path / "test_x.py", "test_y", {"limit_mb": 512, "expect_peak_mb": (220, 330)})
        failure = _peak_gate_failure(item, 340.0)
        assert failure is not None and "outside the expected band" in failure

    def test_peak_below_band_fails(self, tmp_path):
        # Below-band means the workload or measurement is broken (e.g. a
        # 100 MB allocation reported as 60 MB) — must fail, not pass.
        item = _FakeItem(tmp_path / "test_x.py", "test_y", {"limit_mb": 512, "expect_peak_mb": (220, 330)})
        failure = _peak_gate_failure(item, 60.0)
        assert failure is not None and "outside the expected band" in failure

    def test_malformed_band_fails_loudly(self, tmp_path):
        for bad_band in (123, (330, 220), ("a", "b"), (1, 2, 3), (True, 330)):
            item = _FakeItem(tmp_path / "test_x.py", "test_y", {"limit_mb": 512, "expect_peak_mb": bad_band})
            failure = _peak_gate_failure(item, 250.0)
            assert failure is not None and "expect_peak_mb" in failure, bad_band

    def test_marker_band_takes_precedence_over_manifest(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"postgres": {"test_x.py::test_y": 100}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y", {"limit_mb": 512, "expect_peak_mb": (200, 400)})
        # 300 MB violates the manifest baseline (100 + 20%) but sits inside
        # the explicit marker band — the marker wins.
        assert _peak_gate_failure(item, 300.0) is None


class TestBaselineManifestGate:
    def test_peak_over_ceiling_fails(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"defaults": {"tolerance_pct": 20}, "postgres": {"test_x.py::test_y": 250}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        failure = _peak_gate_failure(item, 301.0)  # ceiling is 300
        assert failure is not None
        assert "exceeds the recorded baseline" in failure
        assert "250" in failure

    def test_peak_under_ceiling_passes(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"defaults": {"tolerance_pct": 20}, "postgres": {"test_x.py::test_y": 250}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _peak_gate_failure(item, 299.0) is None

    def test_peak_well_below_baseline_passes(self, tmp_path, monkeypatch, capsys):
        # Improvements never fail — they only print a tighten-me hint.
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"postgres": {"test_x.py::test_y": 250}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _peak_gate_failure(item, 120.0) is None
        assert "consider tightening" in capsys.readouterr().out

    def test_parametrized_test_name_is_the_key(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"postgres": {"test_x.py::test_y[10x10M]": 250}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y[10x10M]")
        assert _peak_gate_failure(item, 700.0) is not None
        other_param = _FakeItem(tmp_path / "test_x.py", "test_y[5x100M]")
        assert _peak_gate_failure(other_param, 700.0) is None  # no entry for this param

    def test_dict_entry_with_custom_tolerance(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(
            tmp_path,
            {
                "defaults": {"tolerance_pct": 20},
                "postgres": {"test_x.py::test_y": {"peak_mb": 200, "tolerance_pct": 5}},
            },
        )
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _lookup_peak_baseline(item) == (200.0, 5.0)
        assert _peak_gate_failure(item, 211.0) is not None  # ceiling is 210
        assert _peak_gate_failure(item, 209.0) is None

    def test_other_datasource_entry_does_not_apply(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "sqlserver")
        _write_manifest(tmp_path, {"postgres": {"test_x.py::test_y": 100}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _peak_gate_failure(item, 900.0) is None

    def test_no_manifest_means_no_gate(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _peak_gate_failure(item, 900.0) is None

    def test_unreadable_manifest_is_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        (tmp_path / BASELINES_FILENAME).write_text("{not json")
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _peak_gate_failure(item, 900.0) is None

    def test_malformed_entry_is_ignored(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_DATASOURCE", "postgres")
        _write_manifest(tmp_path, {"postgres": {"test_x.py::test_y": "lots"}})
        item = _FakeItem(tmp_path / "test_x.py", "test_y")
        assert _lookup_peak_baseline(item) is None
        assert _peak_gate_failure(item, 900.0) is None
