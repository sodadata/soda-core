"""Phase 5 measurement-sanity tests.

Verify the plugin's peak reporting is honest:

* test_baseline_no_alloc — soda imports + minimal test body, no big alloc.
  Establishes the per-test "startup floor" we should subtract from data-driven
  peaks before claiming "data costs X MB".

* test_allocate_100mb_baseline — allocate exactly 100 MB of bytes, hold it.
  Plugin should report peak ≈ startup floor + 100 MB. Any larger = the plugin
  is double-counting or measurement is wrong.

If these two numbers don't agree with the math, the FRQ multiplier numbers
need a second look before drawing conclusions about libpq buffering.

The expect_peak_mb bands make the sanity check self-enforcing: the plugin
FAILS the test when the reported peak falls outside the band, in either
direction. Below-band means the measurement missed the allocation (a dead
pipeline used to report these as ~floor-only passes); above-band means
double-counting or a bloated import floor. Bands assume the all-adapter
image floor of ~150-175 MB (postgres/sqlserver ~150, snowflake ~174) —
if the image floor legitimately changes, update the bands deliberately.
"""

import pytest


@pytest.mark.memory_container(limit_mb=256, expect_peak_mb=(100, 230))
def test_baseline_no_alloc():
    # Minimal body. Reported peak is the "soda + pytest startup floor" we
    # subtract from data-driven peaks elsewhere. The real assertion is the
    # expect_peak_mb band, enforced out-of-process by the memory_container plugin.
    pass


@pytest.mark.memory_container(limit_mb=512, expect_peak_mb=(220, 330))
def test_allocate_100mb_baseline():
    payload = bytearray(100 * 1024 * 1024)  # 100 MB exactly
    # Touch every page so the OS actually maps it (lazy allocation otherwise).
    for i in range(0, len(payload), 4096):
        payload[i] = 1
    assert len(payload) == 100 * 1024 * 1024


@pytest.mark.memory_container(limit_mb=512, expect_peak_mb=(320, 430))
def test_allocate_200mb_baseline():
    payload = bytearray(200 * 1024 * 1024)
    for i in range(0, len(payload), 4096):
        payload[i] = 1
    assert len(payload) == 200 * 1024 * 1024
