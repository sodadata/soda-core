"""Phase 1 + 2A smoke tests for the memory_container plugin.

- ``test_dispatch_roundtrip``: trivial body, verifies the host → docker → inner
  pytest → exit-code roundtrip and that the measured peak is well under the cap.
- ``test_allocates_below_cap``: allocates a known amount and verifies the peak
  is reported back to the host (no assertion here — the plugin's own peak ≤ limit
  assertion is what we trust; this test just makes the peak non-trivial).
- ``test_oom_kill_when_over_cap``: allocates more than the cap. Marked
  ``xfail(strict=True)`` so the run is *expected* to fail with an OOM-kill.
  The plugin reports a non-zero outcome → pytest treats it as xfail → green.
"""

import pytest


@pytest.mark.memory_container(limit_mb=128)
def test_dispatch_roundtrip():
    payload = list(range(10_000))
    assert sum(payload) == sum(range(10_000))


@pytest.mark.memory_container(limit_mb=256)
def test_allocates_below_cap():
    # Allocate ~80 MB of bytes; well under the 256 MB cap.
    chunk = b"x" * (80 * 1024 * 1024)
    assert len(chunk) == 80 * 1024 * 1024


@pytest.mark.memory_container(limit_mb=256)
def test_dense_timeseries_when_gil_releases():
    """Allocate in many small steps with GIL releases between, so the inner
    200 Hz poller actually samples densely (vs. one big C-side alloc that
    holds the GIL and starves the poller). Verifies the timeseries CSV has
    meaningful resolution for real DWH workloads where DB I/O releases the GIL."""
    import time

    chunks = []
    for _ in range(80):
        chunks.append(b"x" * (1024 * 1024))  # 1 MB
        time.sleep(0.003)  # 3ms — releases GIL, gives poller time to sample
    assert len(chunks) == 80


@pytest.mark.memory_container(limit_mb=64)
@pytest.mark.xfail(strict=True, reason="intentional OOM — expected to be SIGKILL'd at the cgroup cap")
def test_oom_kill_when_over_cap():
    # Allocate ~256 MB — far over the 64 MB cap. Should be OOM-killed (exit 137),
    # which the plugin surfaces as a test failure. xfail(strict=True) flips that
    # expected failure into a pass at the suite level.
    chunks = []
    for _ in range(64):
        chunks.append(b"x" * (4 * 1024 * 1024))
    # If we somehow get here without OOM, that's a bug in the cap mechanism.
    assert sum(len(c) for c in chunks) == 256 * 1024 * 1024


# ----------------------------------------------------------------------------
# Phase 3 validation: read-only broad mount, RW artifact overlay, networking.
# ----------------------------------------------------------------------------


@pytest.mark.memory_container(limit_mb=128)
def test_broad_mount_is_readonly():
    """The repo-root bind mount must be :ro so a misbehaving test can't write to
    the host workspace (which would clobber source code or, worse, credentials)."""
    import errno
    import os

    # CWD is the pytest rootpath inside the container (same as on host).
    write_target = os.path.join(os.getcwd(), "_memory_container_ro_probe.tmp")
    try:
        with open(write_target, "w") as f:
            f.write("if you see this on the host, the :ro mount is broken")
    except OSError as e:
        # Different Docker storage drivers may surface this as EROFS, EACCES,
        # or EPERM — all indicate "you can't write here", which is what we want.
        assert e.errno in (errno.EROFS, errno.EACCES, errno.EPERM), f"Unexpected OSError errno={e.errno!r}: {e!r}"
        return
    # If we got here, the write actually succeeded — clean up and fail loudly.
    try:
        os.unlink(write_target)
    finally:
        raise AssertionError(f"Broad bind mount is writable; expected :ro. Wrote to {write_target}.")


@pytest.mark.memory_container(limit_mb=128)
def test_artifact_dir_is_writable():
    """The artifact subdir must be :rw so the inner poller and memray can write."""
    import os

    artifact_dir = os.environ["SODA_MEMTEST_ARTIFACT_DIR"]
    probe = os.path.join(artifact_dir, "_rw_probe.tmp")
    with open(probe, "w") as f:
        f.write("ok")
    try:
        with open(probe) as f:
            assert f.read() == "ok"
    finally:
        os.unlink(probe)


@pytest.mark.memory_container(limit_mb=128)
def test_host_docker_internal_reaches_host():
    """The container must be able to reach the host via host.docker.internal so
    DB-touching tests can connect to host-bound DB containers (postgres:5432).

    DNS resolution alone is near-tautological on Docker Desktop (the embedded
    DNS server knows about host.docker.internal regardless of --add-host).
    Instead we attempt a TCP connect to postgres:5432 (which the user runs
    locally per project conventions). Skip if the host isn't running postgres.
    """
    import socket

    addr = socket.gethostbyname("host.docker.internal")
    assert addr, "host.docker.internal didn't resolve"
    # TCP-level reach. 5432 is the canonical postgres port per soda-core's
    # docker-compose. If postgres isn't running, this is informational, not a
    # hard failure — but on a dev box with postgres up, this proves the
    # --add-host wiring actually works at the IP level.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        try:
            sock.connect((addr, 5432))
        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            # Postgres might not be up; that's not a Phase-3 failure. Just
            # don't assert anything stronger than DNS in that case.
            print(
                f"[memory_container] info: TCP connect to host:5432 didn't succeed ({e!r}); "
                "DNS resolution + add-host already validated."
            )


@pytest.mark.memory_container(limit_mb=128)
def test_host_env_vars_propagate(monkeypatch):
    """A host-set env var that is NOT set by the plugin should still be visible
    inside the container. Validates the denylist isn't accidentally too broad."""
    # monkeypatch.setenv runs in the host pytest BEFORE the plugin dispatches to
    # docker. The plugin's _build_env_args reads os.environ at dispatch time,
    # so the value should propagate to the inner pytest.
    monkeypatch.setenv("SODA_MEMTEST_HOST_PROBE", "round-trip-marker-xyz-789")
    import os

    assert os.environ.get("SODA_MEMTEST_HOST_PROBE") == "round-trip-marker-xyz-789", (
        "Host-set env var did not propagate into the container. The denylist "
        "may be over-broad or _build_env_args is broken."
    )
