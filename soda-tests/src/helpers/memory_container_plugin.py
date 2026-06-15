"""Pytest plugin: re-dispatch marked tests into a memory-capped Docker container.

Tests decorated with ``@pytest.mark.memory_container(limit_mb=N)`` are re-run
inside a fresh container started with ``--memory=Nm --memory-swap=Nm``. This
mirrors Kubernetes cgroup accounting and gives a true OOM-kill (SIGKILL,
exit 137) at the cap rather than ``resource.setrlimit``'s soft ``MemoryError``.

Activation: ``SODA_MEMORY_TESTS=true`` (matches the existing ``SODA_NIGHTLY``
pattern). Without it, marked tests are skipped so default local + CI runs
keep working.

Three measurement sources are reconciled into a single peak:

1. **Inner cgroup poller** (primary, 200 Hz): a thread inside the container
   reads ``/sys/fs/cgroup/memory.current`` and writes ``peak.txt`` + a
   timeseries CSV to a bind-mounted artifact directory. Flushed every 50 ms
   so we don't lose data if the inner process is SIGKILL'd mid-write.
2. **Kernel ground truth** (best when it exists): at clean inner shutdown,
   we also read ``/sys/fs/cgroup/memory.peak`` (kernel-maintained max of
   ``memory.current``) and write it to ``cgroup_peak.txt``. This is the
   authoritative number when the test exits normally.
3. **Host docker stats poller** (OOM safety net, ~5 Hz): a thread on the
   host runs ``docker stats --no-stream`` against the container's CID and
   tracks max. Survives SIGKILL because it lives outside the container.

The assertion uses ``max(...)`` across whatever sources are available.

Dispatch happens at ``pytest_runtest_protocol`` so host-side autouse
fixtures do NOT run on the host before re-entering the container.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

import pytest
from _pytest.reports import TestReport

logger = logging.getLogger(__name__)

ACTIVATION_ENV = "SODA_MEMORY_TESTS"
INNER_ENV_VAR = "SODA_IN_MEMORY_CONTAINER"
ARTIFACT_DIR_ENV = "SODA_MEMTEST_ARTIFACT_DIR"
ARTIFACTS_ROOT_ENV = "SODA_MEMTEST_ARTIFACTS_ROOT"
MEMRAY_ENABLE_ENV = "SODA_MEMTEST_MEMRAY"
# Fixed-schema env var consumed by DataSourceTestHelper._create_schema_name.
# When setup_outside=True, plugin sets this to a deterministic name on the
# host helper and passes the same value into the container so both sides
# compute the same schema name — required for ensure_test_table's dedup to
# recognise the table that __prepare_outside__ created.
FIXED_SCHEMA_ENV = "SODA_MEMTEST_FIXED_SCHEMA"
# Shared schema name memory tests always land in. Every memory test's
# host helper + in-container helper computes the same value via
# FIXED_SCHEMA_ENV, so unique-table names dedup across runs and stale
# tables can be cleaned up by a separate sweep. The lowercase, no-prefix
# name keeps it identifiable for postgres / sqlserver / bigquery /
# snowflake / databricks alike.
MEMORY_TEST_SCHEMA_NAME = "dev_memory_testing"
# Set by the plugin (alongside FIXED_SCHEMA_ENV) when setup_outside=True, to
# prevent the in-container DataSourceTestHelper from dropping the schema the
# host has just populated via __prepare_outside__ (the helper's CI-mode
# `start_test_session_ensure_schema` would otherwise drop it).
SKIP_SCHEMA_DROP_ENV = "SODA_MEMTEST_SKIP_SCHEMA_DROP"
# Read by ``DataSourceTestHelper._drop_test_table``. When set, every table-
# level DROP is suppressed (session-end cleanup, obsolete-table eviction
# inside ensure_test_table, per-test ``__finalize_outside__`` hooks). The
# plugin turns this on for every memory test so the dev_memory_testing
# schema's expensive fixture tables persist for the next run.
KEEP_TABLES_ENV = "SODA_MEMTEST_KEEP_TABLES"
# Set by the plugin after __prepare_outside__ runs, when setup_outside=True.
# Carries a JSON list of the unique_names of every test table the host helper
# ensured during prepare. The in-container test code reads this and builds a
# *stub* TestTableSpecification (with the matching unique_name and no rows)
# so the container does not rebuild 100+ MB of row tuples just to look up the
# already-inserted table. Without this, peak measurements were contaminated
# by ~payload_bytes × n_rows of Python tuple/string memory (peak-resident at
# the moment of contract verification).
FORCED_TABLE_NAMES_ENV = "SODA_MEMTEST_FORCED_TABLE_NAMES"
DEFAULT_IMAGE_TAG = "soda-memtest:latest"

_PLUGIN_FILE = Path(__file__).resolve()
# .../soda_claude/soda-core/soda-tests/src/helpers/memory_container_plugin.py
#                  ^ parents[4] = soda_claude (the parent that holds both repos)
REPO_PARENT = _PLUGIN_FILE.parents[4]
assert (REPO_PARENT / "soda-core").is_dir(), (
    f"REPO_PARENT={REPO_PARENT} doesn't look like the soda_claude root "
    "(expected ./soda-core/ to exist). Did the plugin move?"
)

# Artifacts land under the active pytest rootpath in .memory_test_artifacts/
# (gitignored). Override with SODA_MEMTEST_ARTIFACTS_ROOT for a custom location.
ARTIFACTS_SUBDIR = ".memory_test_artifacts"

# Per-datasource peak baselines, looked up next to the test file. A test whose
# peak exceeds its recorded baseline + tolerance FAILS even though it stayed
# under the cgroup cap — the cap is an OOM guard, not a regression gate.
BASELINES_FILENAME = "memory_baselines.json"

# Env values that count as "on" for opt-in flags.
_TRUTHY = {"1", "true", "yes", "on", "y", "t"}


def _running_in_ci() -> bool:
    """True on CI runners (GitHub Actions sets both; CI is the de-facto
    cross-provider convention). Memory tests are local-only by policy."""
    return os.environ.get("CI", "").lower() in _TRUTHY or os.environ.get("GITHUB_ACTIONS", "").lower() in _TRUTHY


ENV_DENYLIST = {
    "PATH",
    "HOME",
    "USER",
    "PWD",
    "OLDPWD",
    "SHELL",
    "SHLVL",
    "_",
    "TMPDIR",
    "TMP",
    "TEMP",
    "LANG",
    "TERM",
    "COLORTERM",
    "VIRTUAL_ENV",
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONBREAKPOINT",
    "COMMAND_MODE",
    "PYTEST_CURRENT_TEST",
    "PYTEST_VERSION",
}
ENV_DENYLIST_PREFIXES = ("DYLD_", "LC_", "XPC_", "__CF", "PYTEST_XDIST_")

_LOCALHOST_RE = re.compile(r"^(localhost|127\.0\.0\.1)(:\d+)?$")

# Env-var suffixes that typically point at credential / keyfile paths on the
# host. When such a var holds an absolute path that exists, we bind-mount it
# read-only into the container at the same path so the inner test resolves it
# transparently.
_CREDENTIAL_ENV_SUFFIXES = ("_PATH", "_CREDENTIALS", "_KEYFILE", "_KEY_FILE")
_CREDENTIAL_ENV_EXACT = frozenset(
    {
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "BOTO_CONFIG",
        "KUBECONFIG",
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "SNOWFLAKE_CONNECTIONS_FILE",
    }
)

# Well-known credential directories — auto-mounted RO if they exist on the host.
_WELL_KNOWN_CREDENTIAL_DIRS = ("~/.aws", "~/.config/gcloud", "~/.azure")

# Paths that must NEVER be auto-mounted even if some `_PATH` env var references
# them. Defence in depth against a misnamed/malicious env var causing us to
# leak SSH keys, shell history, or browser data into the container's RO mount
# (where any `pip install`'d package in the image could read them). The check
# is a prefix match after resolving symlinks.
_CREDENTIAL_MOUNT_DENYLIST = tuple(
    str(Path(p).expanduser())
    for p in (
        "~/.ssh",
        "~/.gnupg",
        "~/.netrc",
        "~/.bash_history",
        "~/.zsh_history",
        "~/.python_history",
        "~/.docker/config.json",
        "~/Library/Application Support",  # macOS browser/app data
        "~/.mozilla",
        "~/.config/google-chrome",
    )
)

# Inner poller: 200 Hz, flush peak.txt every 50 ms.
_INNER_POLL_HZ = 200
_INNER_FLUSH_INTERVAL_S = 0.05

# Host poller: 5 Hz docker stats.
_HOST_POLL_HZ = 5

_CGROUP_DIR = Path("/sys/fs/cgroup")


# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "memory_container(limit_mb): run the test inside a fresh Docker "
        "container with --memory=limit_mbm (and --memory-swap matched, so no "
        "swap — matches K8s). Asserts measured peak ≤ limit_mb. Skipped "
        "unless SODA_MEMORY_TESTS=true.",
    )


# ---------------------------------------------------------------------------
# Inner-container side: cgroup poller, auto-started on session start.
# ---------------------------------------------------------------------------


_INNER_POLLER: Optional["_CgroupPoller"] = None

# Rename detection for the baseline gate: a renamed test silently loses its
# memory_baselines.json entry (the gate fails open). Dispatched test keys and
# consulted manifests are tracked so the session can flag manifest entries
# whose test function no longer exists in a file that DID run.
_DISPATCHED_MEMORY_KEYS: set[str] = set()
_CONSULTED_MANIFESTS: dict[Path, str] = {}


def pytest_sessionstart() -> None:
    global _INNER_POLLER
    if os.environ.get(INNER_ENV_VAR) != "1":
        return
    artifact_dir_str = os.environ.get(ARTIFACT_DIR_ENV)
    if not artifact_dir_str:
        return
    artifact_dir = Path(artifact_dir_str)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _INNER_POLLER = _CgroupPoller(artifact_dir)
    _INNER_POLLER.start()


def pytest_sessionfinish() -> None:
    if _INNER_POLLER is not None:
        _INNER_POLLER.stop_and_flush()
    _warn_stale_baseline_keys()


def _warn_stale_baseline_keys() -> None:
    """Flag baseline entries whose test function vanished from a file that ran.

    Heuristic: an entry is stale when its FILE had at least one dispatched
    test this session but NO dispatched test shares the entry's function
    base (file.py::test_name). Selecting single params with -k keeps the
    base matched, so partial selections don't false-positive; renaming or
    deleting the function does trip it — that rename otherwise silently
    un-gates the test (the gate fails open on unknown keys).
    """
    if not _DISPATCHED_MEMORY_KEYS or not _CONSULTED_MANIFESTS:
        return
    dispatched_files = {key.split("::", 1)[0] for key in _DISPATCHED_MEMORY_KEYS}
    dispatched_bases = {key.split("[", 1)[0] for key in _DISPATCHED_MEMORY_KEYS}
    for manifest_path, datasource in _CONSULTED_MANIFESTS.items():
        try:
            per_datasource = json.loads(manifest_path.read_text()).get(datasource) or {}
        except (OSError, json.JSONDecodeError):
            continue
        stale = [
            key
            for key in per_datasource
            if key.split("::", 1)[0] in dispatched_files and key.split("[", 1)[0] not in dispatched_bases
        ]
        if stale:
            print(
                f"\n[memory_container] WARNING: {manifest_path} has '{datasource}' baseline entries "
                f"whose test no longer exists in a file that ran this session — those tests are now "
                f"silently UNGATED (renamed or deleted?): {stale}"
            )


# Filename (under each test's artifact dir) holding the peak-bytes integer the
# in-container poller writes and the host-side reconciliation reads.
_PEAK_FILENAME = "peak.txt"


class _CgroupPoller(threading.Thread):
    """Reads /sys/fs/cgroup/memory.current at ~200 Hz and writes peak + timeseries."""

    def __init__(self, artifact_dir: Path) -> None:
        super().__init__(daemon=True, name="memtest-cgroup-poller")
        self.artifact_dir = artifact_dir
        self._stop_event = threading.Event()
        self._peak = 0
        self._memory_current = _CGROUP_DIR / "memory.current"
        self._memory_peak = _CGROUP_DIR / "memory.peak"
        self.peak_path = artifact_dir / _PEAK_FILENAME
        self.timeseries_path = artifact_dir / "rss_timeseries.csv"
        self.cgroup_peak_path = artifact_dir / "cgroup_peak.txt"
        # Pre-create files so they exist even if the poller never runs.
        self.peak_path.write_text("0\n")

    def run(self) -> None:
        if not self._memory_current.exists():
            logger.warning("memory.current not found at %s — poller inert", self._memory_current)
            return

        interval = 1.0 / _INNER_POLL_HZ

        # Hold the cgroup file open and lseek(0) per read — avoids ~tens of µs
        # of open/close overhead per iteration that otherwise dominate at 200 Hz.
        try:
            mem_fd = os.open(str(self._memory_current), os.O_RDONLY)
        except OSError as exc:
            logger.warning("Could not open %s: %s", self._memory_current, exc)
            return

        # Block-buffered writes; flush coupled to the same cadence as peak.txt
        # so we don't lose more than _INNER_FLUSH_INTERVAL_S of data on SIGKILL.
        try:
            ts_file = open(self.timeseries_path, "w", buffering=64 * 1024)
        except OSError as exc:
            logger.warning("Could not open timeseries file %s: %s", self.timeseries_path, exc)
            os.close(mem_fd)
            return
        ts_file.write("timestamp_ns,rss_bytes\n")

        start_ns = time.monotonic_ns()
        last_flush_s = time.monotonic()
        buf = bytearray(64)  # reused read buffer

        try:
            while not self._stop_event.is_set():
                try:
                    os.lseek(mem_fd, 0, os.SEEK_SET)
                    n = os.readv(mem_fd, [buf])
                except OSError:
                    break  # cgroup gone — bail out
                try:
                    current = int(bytes(buf[:n]).strip())
                except ValueError:
                    break

                ts_ns = time.monotonic_ns() - start_ns
                ts_file.write(f"{ts_ns},{current}\n")

                if current > self._peak:
                    self._peak = current

                now_s = time.monotonic()
                if now_s - last_flush_s >= _INNER_FLUSH_INTERVAL_S:
                    ts_file.flush()
                    self._write_peak_atomic()
                    last_flush_s = now_s

                if self._stop_event.wait(interval):
                    break
        finally:
            try:
                ts_file.flush()
                ts_file.close()
            except OSError:
                pass
            try:
                os.close(mem_fd)
            except OSError:
                pass

    def _write_peak_atomic(self) -> None:
        # Write to a temp file then rename to avoid partial-read on SIGKILL.
        tmp = self.peak_path.with_suffix(".txt.tmp")
        try:
            tmp.write_text(f"{self._peak}\n")
            tmp.replace(self.peak_path)
        except OSError:
            pass

    def stop_and_flush(self) -> None:
        self._stop_event.set()
        self.join(timeout=2.0)
        self._write_peak_atomic()
        # Also capture the kernel's own peak counter as ground truth.
        try:
            cgroup_peak = int(self._memory_peak.read_text().strip())
            self.cgroup_peak_path.write_text(f"{cgroup_peak}\n")
        except (OSError, ValueError):
            pass


# ---------------------------------------------------------------------------
# Host side: dispatch + host docker-stats poller + assertion.
# ---------------------------------------------------------------------------


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: Optional[pytest.Item]) -> Optional[bool]:
    marker = item.get_closest_marker("memory_container")
    if marker is None:
        return None  # Default protocol — host runs the test normally.

    if os.environ.get(INNER_ENV_VAR) == "1":
        return None  # Inside the container — let the default protocol invoke the body.

    # Honor skip/skipif markers BEFORE dispatching to docker. This hook takes
    # over the protocol before pytest's skipping plugin evaluates them;
    # without this, a skipif'd memory test was dispatched anyway — the inner
    # pytest (same env) skipped it and exited 0, and the plugin reported a
    # phantom "pass" whose peak was just the container's ~150 MB import
    # floor (observed: all 13 test_scaling_profiling_only variants).
    try:
        from _pytest.skipping import evaluate_skip_marks

        skip_result = evaluate_skip_marks(item)
    except ImportError:  # pytest internals moved — fall back to dispatching
        skip_result = None
    if skip_result is not None:
        _emit_skipped(item, skip_result.reason)
        return True

    # Memory tests are LOCAL-ONLY by policy: they need the docker memtest
    # image, developer credentials, and a machine whose memory the
    # measurements assume to own. Refuse to run in CI even if someone sets
    # the activation env var there — checked BEFORE activation on purpose.
    if _running_in_ci():
        _emit_skipped(item, "memory_container tests are local-only — refusing to run in CI")
        return True

    if os.environ.get(ACTIVATION_ENV, "").lower() not in _TRUTHY:
        _emit_skipped(item, f"set {ACTIVATION_ENV}=true to enable memory_container tests")
        return True

    limit_mb = marker.kwargs.get("limit_mb")
    if not isinstance(limit_mb, int) or limit_mb <= 0:
        _emit_failed(
            item,
            when="setup",
            longrepr=(
                f"@pytest.mark.memory_container requires a positive int limit_mb. "
                f"Got {limit_mb!r} ({type(limit_mb).__name__})."
            ),
        )
        return True

    # All memory tests share a single schema (``dev_memory_testing``). Both
    # the host and the in-container helper read this same env var, so they
    # land in the same schema and unique-table dedup just works. Stale
    # tables are expected to be cleaned up by an external sweep.
    prev_fixed_schema = os.environ.get(FIXED_SCHEMA_ENV)
    os.environ[FIXED_SCHEMA_ENV] = MEMORY_TEST_SCHEMA_NAME

    # Memory test fixtures are huge (hundreds of MB). The whole point of the
    # shared ``dev_memory_testing`` schema is "create once, reuse forever" —
    # rebuilding tables every run would burn minutes per test and hammer the
    # source DB. SKIP_SCHEMA_DROP_ENV suppresses both the CI-mode drop at
    # session start (``start_test_session_ensure_schema``) AND the session-
    # end drop (``end_test_session_drop_schema``). KEEP_TABLES_ENV neuters
    # ``_drop_test_table`` so per-test ``__finalize_outside__`` hooks and
    # the obsolete-table sweep inside ``ensure_test_table`` also leave the
    # cached fixtures alone. Both set at protocol level so they apply to
    # every memory test, with or without setup_outside.
    prev_skip_drop = os.environ.get(SKIP_SCHEMA_DROP_ENV)
    os.environ[SKIP_SCHEMA_DROP_ENV] = "1"
    prev_keep_tables = os.environ.get(KEEP_TABLES_ENV)
    os.environ[KEEP_TABLES_ENV] = "1"

    # Optional "setup outside the container" hook: heavy fixture work (creating
    # large test tables, populating data) runs on the host before docker
    # dispatch and is NOT included in the capped memory measurement. The test
    # module declares __prepare_outside__ / __finalize_outside__ functions
    # which receive a DataSourceTestHelper + the parametrize kwargs.
    setup_outside_state = None
    if bool(marker.kwargs.get("setup_outside", False)):
        try:
            setup_outside_state = _run_prepare_outside(item)
        except Exception as exc:
            import traceback as _tb

            _emit_failed(
                item,
                when="setup",
                longrepr=(
                    f"memory_container setup_outside failed for {item.nodeid}:\n"
                    f"{type(exc).__name__}: {exc}\n\n{_tb.format_exc()}"
                ),
            )
            _restore_env(FIXED_SCHEMA_ENV, prev_fixed_schema)
            _restore_env(SKIP_SCHEMA_DROP_ENV, prev_skip_drop)
            _restore_env(KEEP_TABLES_ENV, prev_keep_tables)
            return True

    _DISPATCHED_MEMORY_KEYS.add(f"{Path(str(item.fspath)).name}::{item.name}")
    try:
        _dispatch_and_emit_reports(item, limit_mb)
    finally:
        if setup_outside_state is not None:
            _run_finalize_outside(setup_outside_state)
        _restore_env(FIXED_SCHEMA_ENV, prev_fixed_schema)
        _restore_env(SKIP_SCHEMA_DROP_ENV, prev_skip_drop)
        _restore_env(KEEP_TABLES_ENV, prev_keep_tables)
    return True


def _run_prepare_outside(item: pytest.Item) -> dict:
    """Create a host-side DataSourceTestHelper, look up the test module's
    ``__prepare_outside__`` function, and call it with the test's parametrize
    kwargs. Returns state needed by ``_run_finalize_outside``.

    Schema-name propagation: the host helper computes its schema name via the
    SAME logic the test helper would normally use (``DataSourceTestHelper.
    _create_schema_name`` — typically ``dev_<USER>`` locally or
    ``ci_<branch>_…`` under CI). We capture that name AFTER the helper is
    constructed, then export it via ``SODA_MEMTEST_FIXED_SCHEMA`` so the
    in-container helper reads the same value and ``ensure_test_table`` can
    dedup against the table the host already created.
    """
    test_module = item.module
    prepare_fn = getattr(test_module, "__prepare_outside__", None)
    if prepare_fn is None:
        raise ValueError(
            f"memory_container(setup_outside=True) requires " f"__prepare_outside__ in {test_module.__name__}"
        )
    finalize_fn = getattr(test_module, "__finalize_outside__", None)

    # Extract parametrize values to forward as kwargs.
    parametrize_kwargs: dict = {}
    if hasattr(item, "callspec") and item.callspec is not None:
        parametrize_kwargs = dict(item.callspec.params)

    # Create the host-side helper using the SAME constructor and lifecycle as
    # the `data_source_test_helper_session` fixture in helpers/test_fixtures.py.
    # FIXED_SCHEMA_ENV is set by the caller (pytest_runtest_protocol) so the
    # helper's cached `_base_schema_name` picks up ``dev_memory_testing``.
    from helpers.data_source_test_helper import DataSourceTestHelper

    test_datasource = os.environ.get("TEST_DATASOURCE", "postgres")
    helper = DataSourceTestHelper.create(test_datasource, name="primary_datasource")
    helper.start_test_session()

    schema_name = helper._create_schema_name()

    # SKIP_SCHEMA_DROP_ENV is set by the protocol-level handler so the
    # dev_memory_testing schema + tables persist across runs for both the
    # host helper and the in-container helper. Nothing to do here.

    prev_forced_names = os.environ.get(FORCED_TABLE_NAMES_ENV)

    # Memory tests build huge fixture tables (hundreds of MB of payload).
    # Activate the registered bulk-inserter so table data is loaded via the
    # adapter's native optimised path (COPY / fast_executemany / load jobs)
    # instead of inline SQL — the inline path trips cloud adapter request
    # limits at these sizes. The flag is reset right after prepare_fn so no
    # other fixture flow ever sees it on.
    from helpers.data_source_test_helper import (
        activate_fixture_bulk_inserter,
        deactivate_fixture_bulk_inserter,
    )

    bulk_prev = activate_fixture_bulk_inserter()
    try:
        prepare_fn(helper, **parametrize_kwargs)
    except Exception:
        deactivate_fixture_bulk_inserter(bulk_prev)
        # Tear down the helper on prepare failure so we don't leak the session.
        try:
            helper.end_test_session(exception=None)
        except Exception as teardown_exc:
            # Mirror the warning to stderr too — pytest's default log_cli=false
            # config keeps logger output out of the user's view unless the
            # test fails, but teardown failures need to be visible.
            msg = (
                f"[memory_container] setup_outside teardown after prepare failure "
                f"also raised {type(teardown_exc).__name__}: {teardown_exc}"
            )
            logger.warning(msg)
            print(msg, file=sys.stderr)
        raise
    else:
        deactivate_fixture_bulk_inserter(bulk_prev)

    # Capture every table the host helper ensured during prepare, so the
    # container can skip rebuilding their (potentially huge) row payloads.
    ensured_names = [tbl.unique_name for tbl in helper._ensured_test_tables.values()]
    os.environ[FORCED_TABLE_NAMES_ENV] = json.dumps(ensured_names)

    return {
        "helper": helper,
        "finalize_fn": finalize_fn,
        "params": parametrize_kwargs,
        "schema_name": schema_name,
        "prev_forced_names_env": prev_forced_names,
    }


def _restore_env(name: str, prev_value: Optional[str]) -> None:
    """Restore an env var to its prior state (unset if it was unset, else set
    back to the captured value)."""
    if prev_value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = prev_value


def _run_finalize_outside(state: dict) -> None:
    """Call the test module's ``__finalize_outside__`` on the host after the
    main container has finished. Best-effort — swallow exceptions so a
    cleanup failure doesn't mask the test's reported outcome, but log them.
    """
    helper = state["helper"]
    finalize_fn = state["finalize_fn"]
    params = state["params"]
    prev_forced_names = state.get("prev_forced_names_env")

    try:
        if finalize_fn is not None:
            try:
                finalize_fn(helper, **params)
            except Exception as exc:
                msg = (
                    f"[memory_container] __finalize_outside__ raised "
                    f"{type(exc).__name__}: {exc} (continuing teardown — table may leak)"
                )
                logger.warning(msg)
                print(msg, file=sys.stderr)
    finally:
        try:
            helper.end_test_session(exception=None)
        except Exception as teardown_exc:
            msg = (
                f"[memory_container] host-helper end_test_session raised "
                f"{type(teardown_exc).__name__}: {teardown_exc}"
            )
            logger.warning(msg)
            print(msg, file=sys.stderr)
        # Restore env so subsequent unrelated tests don't see stale state.
        # FIXED_SCHEMA_ENV and SKIP_SCHEMA_DROP_ENV are restored by the
        # protocol-level handler.
        _restore_env(FORCED_TABLE_NAMES_ENV, prev_forced_names)


def _dispatch_and_emit_reports(item: pytest.Item, limit_mb: int) -> None:
    item.ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
    item.ihook.pytest_runtest_logreport(report=_build_report(item, when="setup", outcome="passed"))

    start = time.perf_counter()
    outcome, longrepr = _run_in_container(item, limit_mb)
    duration = time.perf_counter() - start

    # Bypassing the default protocol means pytest's own xfail handling
    # (in _pytest/skipping.py) doesn't fire. Apply it ourselves so that
    # @pytest.mark.xfail tests display correctly (XFAIL / XPASS).
    if outcome == "skipped":
        # Runtime pytest.skip() inside the container: render as a proper
        # skip (tuple longrepr) and bypass xfail handling — skip wins.
        longrepr = (str(item.fspath), 0, longrepr or "skipped inside memory container")
        wasxfail = None
    else:
        outcome, longrepr, wasxfail = _apply_xfail(item, outcome, longrepr)
    call_report = _build_report(item, when="call", outcome=outcome, longrepr=longrepr, duration=duration)
    if wasxfail is not None:
        call_report.wasxfail = wasxfail
    item.ihook.pytest_runtest_logreport(report=call_report)
    item.ihook.pytest_runtest_logreport(report=_build_report(item, when="teardown", outcome="passed"))
    item.ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)


def _apply_xfail(item: pytest.Item, outcome: str, longrepr: Optional[str]) -> tuple[str, Optional[str], Optional[str]]:
    """Best-effort xfail support for our protocol-replacement hook.

    Limitation: only ``reason=`` and ``strict=`` kwargs are honored. ``condition``,
    ``raises``, and ``run=False`` are NOT evaluated — using them on a
    memory_container test will produce incorrect XFAIL semantics. If you need
    those, use the standard pytest protocol (i.e. don't put memory_container
    on that test).
    """
    xfail = item.get_closest_marker("xfail")
    if xfail is None:
        return outcome, longrepr, None
    # If the user passed unsupported kwargs, fail loudly rather than silently
    # turn a real failure into a green XFAIL.
    unsupported = set(xfail.kwargs).difference({"reason", "strict"})
    if unsupported:
        raise ValueError(
            f"memory_container does not support xfail({sorted(unsupported)}). "
            "Only 'reason' and 'strict' are honored. See _apply_xfail docstring."
        )
    if xfail.args:
        # Positional condition (e.g. xfail(sys.platform == 'win32', reason=...))
        raise ValueError(
            "memory_container does not support xfail(condition, ...). Use @pytest.mark.skipif for conditional skips."
        )
    reason = xfail.kwargs.get("reason", "") or "expected to fail"
    strict = bool(xfail.kwargs.get("strict", False))
    if outcome == "failed":
        # Matches pytest's standard xfail handling in _pytest/skipping.py:
        # outcome="skipped" + report.wasxfail = reason. Both the terminal
        # reporter and junitxml.append_skipped honor this combo as XFAIL.
        return "skipped", None, reason
    if outcome == "passed" and strict:
        # XPASS in strict mode → real failure.
        return "failed", f"[XPASS(strict)] {reason}", None
    return outcome, longrepr, None


def _run_in_container(item: pytest.Item, limit_mb: int) -> tuple[str, Optional[str]]:
    image = os.environ.get("SODA_MEMTEST_IMAGE", DEFAULT_IMAGE_TAG)
    bind_root = str(REPO_PARENT)
    rootdir = str(item.config.rootpath)

    artifact_dir = _make_artifact_dir(item)
    cidfile = artifact_dir / "container.cid"
    # Memray defaults to ON: flamegraphs are exactly the diagnostic value we
    # want from memory_container runs. Opt out by setting the env var to a
    # falsy value (0, false, no, off, n, f) if speed matters.
    memray_setting = os.environ.get(MEMRAY_ENABLE_ENV, "").lower()
    if memray_setting == "":
        memray_enabled = True
    elif memray_setting in _TRUTHY:
        memray_enabled = True
    else:
        memray_enabled = False
    memray_bin_path = artifact_dir / "memray.bin"

    # Full DEBUG log of everything the in-container test does, written
    # incrementally to the artifact dir REGARDLESS of outcome. Without this,
    # pytest only prints captured log records on failure — passing runs left
    # no SQL/transfer trace, which made post-hoc diagnosis (e.g. the dead
    # FRQ check, the silent AST insert skip) require re-running to failure.
    # ``-s`` (capture off) is what actually delivers it: soda emits its logs
    # through a dedicated stdout handler (no propagation), so they bypass
    # pytest's log_file; with capture disabled they stream live into
    # inner_pytest_stdout.log on every run instead of being buffered
    # in-container and replayed only on failure. (That buffering was itself
    # a measurement artifact — pytest held ~80 MB of captured records on a
    # log-heavy run.) log_file stays as a net for third-party records that
    # do propagate to the root logger.
    inner_debug_log_path = artifact_dir / "inner_debug.log"
    _pytest_args = [
        "--no-header",
        "-p",
        "no:cacheprovider",
        "--tb=short",
        "-s",
        "-o",
        f"log_file={inner_debug_log_path}",
        "-o",
        "log_file_level=DEBUG",
        "-o",
        "log_file_format=%(asctime)s %(levelname)-7s %(name)s %(message)s",
        item.nodeid,
    ]

    inner_cmd: list[str]
    if memray_enabled:
        inner_cmd = [
            "python",
            "-m",
            "memray",
            "run",
            "--force",  # overwrite existing .bin if present
            "-o",
            str(memray_bin_path),
            "-m",
            "pytest",
            *_pytest_args,
        ]
    else:
        inner_cmd = [
            "python",
            "-m",
            "pytest",
            *_pytest_args,
        ]

    env_args, env_file_path = _build_env_args(artifact_dir)
    cmd = [
        "docker",
        "run",
        "--rm",
        f"--cidfile={cidfile}",
        f"--memory={limit_mb}m",
        f"--memory-swap={limit_mb}m",
        "--add-host=host.docker.internal:host-gateway",
        # Broad mount is read-only so a misbehaving test can't write to the
        # host repo or scribble on credential files. Artifact subdir is
        # overlaid as :rw so the inner poller and memray can write outputs.
        "-v",
        f"{bind_root}:{bind_root}:ro",
        "-v",
        f"{artifact_dir}:{artifact_dir}:rw",
        *_build_credential_mount_args(bind_root),
        "-w",
        rootdir,
        *env_args,
        image,
        *inner_cmd,
    ]

    host_poller = _DockerStatsPoller(cidfile)
    host_poller.start()

    # Capture BOTH stdout and stderr (instead of inheriting stdout). Pytest
    # writes test results + tracebacks to stdout, so on failure the user
    # needs to SEE that output. We tee to:
    #   - the host's stdout/stderr (real-time visibility)
    #   - a file in the artifact dir (post-hoc inspection)
    # On failure, the message also includes a tail of the captured output.
    stdout_log = artifact_dir / "inner_pytest_stdout.log"
    stderr_log = artifact_dir / "inner_pytest_stderr.log"
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    # A hung inner test (deadlocked DB connection, infinite loop) previously
    # hung the host suite forever — communicate() had no timeout.
    container_timeout_s = int(os.environ.get("SODA_MEMTEST_CONTAINER_TIMEOUT", "1800"))
    try:
        stdout, stderr = proc.communicate(timeout=container_timeout_s)
    except subprocess.TimeoutExpired:
        _kill_container(cidfile)
        proc.kill()
        stdout, stderr = proc.communicate()
        host_poller.stop()
        host_poller.join(timeout=2.0)
        return "failed", (
            f"memory_container test exceeded SODA_MEMTEST_CONTAINER_TIMEOUT={container_timeout_s}s "
            f"and was killed. Partial output tail:\n{(stdout or '')[-2000:]}"
        )
    except BaseException:
        # Ctrl-C (or any other interruption) used to leave the container
        # running detached — kill it before propagating.
        _kill_container(cidfile)
        proc.kill()
        raise
    finally:
        if env_file_path is not None:
            try:
                env_file_path.unlink()
            except OSError:
                pass
    rc = proc.returncode

    host_poller.stop()
    host_poller.join(timeout=2.0)

    # Stream the captured output to host stdout/stderr AND save to disk.
    if stdout:
        sys.stdout.write(stdout)
        sys.stdout.flush()
        stdout_log.write_text(stdout)
    if stderr:
        sys.stderr.write(stderr)
        sys.stderr.flush()
        stderr_log.write_text(stderr)

    # Generate memray reports unconditionally if the .bin exists — including
    # when the test failed or OOM'd. A flamegraph of an OOM'd run is exactly
    # what you want to look at when diagnosing the OOM.
    memray_reports_produced: dict[str, Path] = {}
    if memray_enabled:
        if memray_bin_path.exists() and memray_bin_path.stat().st_size > 0:
            memray_reports_produced = _generate_memray_reports(image, artifact_dir, memray_bin_path)
        else:
            print(
                f"[memory_container] WARNING: memray was enabled but memray.bin "
                f"is missing or empty for {item.nodeid}. The inner process likely "
                f"crashed before memray could flush. Artifact dir: {artifact_dir}"
            )

    def _memray_paths_blurb() -> str:
        if not memray_reports_produced:
            return ""
        lines = ["\nmemray output:"]
        for label, path in memray_reports_produced.items():
            lines.append(f"  {label}: {path}")
        return "\n".join(lines)

    peak_bytes, peak_source = _resolve_peak(artifact_dir, host_poller.peak_bytes)
    limit_bytes = limit_mb * 1024 * 1024
    artifacts_hint = f"Artifacts: {artifact_dir}"

    # Build a tail-of-output snippet for failure messages so the user doesn't
    # have to scroll back through interleaved pytest output to find why the
    # inner test failed. ~30 lines balances "informative" vs "wall of text".
    def _output_tail() -> str:
        parts: list[str] = []
        if stdout:
            tail = "\n".join(stdout.rstrip("\n").splitlines()[-30:])
            parts.append(f"\n--- inner pytest stdout (last 30 lines) ---\n{tail}")
        if stderr:
            tail = "\n".join(stderr.rstrip("\n").splitlines()[-15:])
            parts.append(f"\n--- inner pytest stderr (last 15 lines) ---\n{tail}")
        return "".join(parts)

    if rc == 137:
        # OOM-kill implies the cap was reached. If we got no measurement (fast
        # OOM beats our pollers), report ≥ limit_mb rather than the misleading 0.
        if peak_bytes < limit_bytes:
            peak_bytes = limit_bytes
            peak_source = f"{peak_source or 'no sample'} (floor: cap reached by OOM-kill)"
        peak_mb = peak_bytes / (1024 * 1024)
        msg = (
            f"OOM-killed at memory limit {limit_mb} MB (exit 137). "
            f"Peak observed: {peak_mb:.1f} MB (source: {peak_source}). "
            f"{artifacts_hint}{_memray_paths_blurb()}{_output_tail()}"
        )
        return "failed", msg

    peak_mb = peak_bytes / (1024 * 1024)

    if rc != 0:
        return "failed", (
            _format_failure(rc, image, stderr) + f"\n{artifacts_hint}{_memray_paths_blurb()}{_output_tail()}"
        )

    if peak_bytes > limit_bytes:
        # cgroup should have prevented this — surface loudly if it ever happens.
        msg = (
            f"Peak {peak_mb:.1f} MB exceeded limit {limit_mb} MB but exit was clean (no OOM-kill). "
            f"Source: {peak_source}. {artifacts_hint}{_memray_paths_blurb()}"
        )
        return "failed", msg

    if _inner_run_skipped(stdout):
        # The body called pytest.skip() at runtime (the static-marker check
        # before dispatch can't see those): the inner pytest exits 0 with
        # '1 skipped' and no test actually ran — the peak is just the
        # container import floor. Report SKIPPED, not a phantom pass.
        # Checked BEFORE measurement integrity: a near-instant skipped run
        # legitimately produces no measurement.
        return "skipped", "inner pytest reported the test as skipped (runtime pytest.skip)"

    if peak_source == "none":
        # Every measurement source degraded (cgroup-v1 host, kernel without
        # memory.peak, poller never started, docker-stats died). A "pass"
        # whose peak is 0.0 MB is not a measurement — fail loudly instead of
        # silently reporting a dead pipeline as green.
        return "failed", (
            f"No memory measurement was captured (all sources degraded) — the test ran but the "
            f"measurement pipeline is dead. Refusing to report an unmeasured pass. {artifacts_hint}"
        )

    # Surface silently-swallowed verification handler exceptions. soda catches
    # post-processing handler errors in check_collections/base.py and logs via
    # logger.error without propagating — the inner test passes (assertions
    # only check check-outcomes, not handler state) but the workload we mean
    # to measure aborted partway through. Without this guard the measurement
    # would silently reflect that aborted pipeline, not the intended one.
    handler_error = _detect_swallowed_handler_error(stdout)
    if handler_error is not None:
        return "failed", (
            f"Inner test passed but a soda verification handler raised silently.\n"
            f"The peak ({peak_mb:.1f} MB / {limit_mb} MB) does NOT reflect the intended "
            f"workload — the pipeline aborted before completion.\n"
            f"See {artifact_dir}/inner_pytest_stdout.log for full context.\n\n"
            f"{handler_error}"
        )

    # Regression gate: the cgroup cap only guards against OOM — a test can
    # silently grow from 230 MB to 900 MB and still "pass" under a 1024 MB
    # cap. Check the peak against an explicit expect_peak_mb marker band or
    # the per-datasource baseline manifest next to the test file.
    gate_failure = _peak_gate_failure(item, peak_mb)
    if gate_failure is not None:
        return "failed", f"{gate_failure}\n{artifacts_hint}{_memray_paths_blurb()}"

    # Pass — log the peak so it's visible in pytest output.
    print(
        f"[memory_container] {item.nodeid}: "
        f"peak {peak_mb:.1f} MB / {limit_mb} MB ({100 * peak_bytes / limit_bytes:.0f}%) "
        f"(source: {peak_source}, artifacts: {artifact_dir})"
    )
    if memray_reports_produced:
        # Surface the memray output paths explicitly so they aren't easy to miss.
        for label, path in memray_reports_produced.items():
            print(f"[memory_container]   memray {label}: {path}")
    return "passed", None


def _peak_gate_failure(item: pytest.Item, peak_mb: float) -> Optional[str]:
    """Regression gate for a clean, measured pass.

    Precedence: an explicit ``expect_peak_mb=(min_mb, max_mb)`` marker kwarg
    (both bounds enforced — below-band means the workload or the measurement
    is not doing what the test assumes), else the per-datasource baseline in
    ``memory_baselines.json`` next to the test file (upper bound enforced;
    well-below is only logged so improvements never fail). Returns a failure
    message, or None when the peak is acceptable.
    """
    marker = item.get_closest_marker("memory_container")
    band = marker.kwargs.get("expect_peak_mb") if marker else None
    if band is not None:
        if (
            not isinstance(band, (tuple, list))
            or len(band) != 2
            or not all(isinstance(bound, (int, float)) and not isinstance(bound, bool) for bound in band)
            or band[0] > band[1]
        ):
            return (
                f"@pytest.mark.memory_container expect_peak_mb must be a (min_mb, max_mb) pair "
                f"with min <= max. Got {band!r}."
            )
        low, high = band
        if not (low <= peak_mb <= high):
            return (
                f"Peak {peak_mb:.1f} MB is outside the expected band [{low}, {high}] MB "
                f"(expect_peak_mb marker). Above-band is a memory regression; below-band means "
                f"the measurement or the workload is not doing what this test assumes."
            )
        return None

    baseline = _lookup_peak_baseline(item)
    if baseline is None:
        return None
    baseline_mb, tolerance_pct = baseline
    ceiling_mb = baseline_mb * (1 + tolerance_pct / 100)
    if peak_mb > ceiling_mb:
        return (
            f"Peak {peak_mb:.1f} MB exceeds the recorded baseline {baseline_mb:g} MB "
            f"+{tolerance_pct:g}% tolerance (ceiling {ceiling_mb:.1f} MB) for "
            f"TEST_DATASOURCE={os.environ.get('TEST_DATASOURCE')!r} in {BASELINES_FILENAME}. "
            f"The test stayed under its cgroup cap, but this is a memory regression. "
            f"If the increase is intentional, update the baseline."
        )
    floor_mb = baseline_mb * (1 - tolerance_pct / 100)
    if peak_mb < floor_mb:
        print(
            f"[memory_container] {item.nodeid}: peak {peak_mb:.1f} MB is well below the "
            f"recorded baseline {baseline_mb:g} MB — consider tightening {BASELINES_FILENAME}."
        )
    return None


def _lookup_peak_baseline(item: pytest.Item) -> Optional[tuple[float, float]]:
    """Return (baseline_mb, tolerance_pct) for this test on the active
    TEST_DATASOURCE, or None when no manifest / no entry applies.

    Manifest shape (lives next to the test file, keyed by file name + test
    name so it is invocation-directory independent)::

        {
          "defaults": {"tolerance_pct": 20},
          "postgres": {
            "test_dwh_frq_fat_rows.py::test_dwh_frq_fat_rows[10x10M]": 275,
            "test_x.py::test_y": {"peak_mb": 540, "tolerance_pct": 10}
          }
        }
    """
    manifest_path = Path(str(item.fspath)).parent / BASELINES_FILENAME
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Ignoring unreadable %s: %s", manifest_path, exc)
        return None
    # Same default as DataSourceTestHelper.create(): an unset TEST_DATASOURCE
    # runs postgres, so the postgres baselines must gate it.
    datasource = os.environ.get("TEST_DATASOURCE", "postgres")
    _CONSULTED_MANIFESTS[manifest_path] = datasource  # rename detection, see _warn_stale_baseline_keys
    per_datasource = manifest.get(datasource)
    if not isinstance(per_datasource, dict):
        return None
    entry = per_datasource.get(f"{Path(str(item.fspath)).name}::{item.name}")
    if entry is None:
        return None
    default_tolerance = manifest.get("defaults", {}).get("tolerance_pct", 20)
    if isinstance(entry, (int, float)) and not isinstance(entry, bool):
        return float(entry), float(default_tolerance)
    if isinstance(entry, dict) and isinstance(entry.get("peak_mb"), (int, float)):
        return float(entry["peak_mb"]), float(entry.get("tolerance_pct", default_tolerance))
    logger.warning("Ignoring malformed baseline entry %r in %s", entry, manifest_path)
    return None


def _resolve_peak(artifact_dir: Path, host_peak_bytes: int) -> tuple[int, str]:
    """Return (peak_bytes, source_label) using the best available signal."""
    cgroup_peak = _read_int(artifact_dir / "cgroup_peak.txt")
    inner_peak = _read_int(artifact_dir / _PEAK_FILENAME)

    sources: list[tuple[int, str]] = []
    if cgroup_peak:
        sources.append((cgroup_peak, "kernel cgroup memory.peak"))
    if inner_peak:
        sources.append((inner_peak, "inner 200Hz poller"))
    if host_peak_bytes:
        sources.append((host_peak_bytes, "host docker stats"))

    if not sources:
        return 0, "none"

    peak, label = max(sources, key=lambda x: x[0])
    return peak, label


def _read_int(path: Path) -> int:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return 0


# Soda emits this exact prefix from check_collections/base.py's exception
# handler around verification-handler invocation. Matching on the literal
# "verification handler" suffix keeps the pattern narrow enough to avoid
# false positives from arbitrary user code that happens to log "Error in".
_HANDLER_ERROR_RE = re.compile(r"Error in .+? verification handler: .*")

# Truncation cap for the failure-message excerpt. Plenty for the message + a
# typical traceback; full context is always available in inner_pytest_stdout.log.
_HANDLER_ERROR_MAX_LEN = 4000


def _detect_swallowed_handler_error(stdout: Optional[str]) -> Optional[str]:
    """If soda's verification-handler error appears anywhere in the inner
    stdout, return a snippet that includes the marker line and the traceback
    that immediately followed. Otherwise return None.

    Used by ``_run_in_container`` to fail tests where soda swallowed a
    handler exception (logged at ERROR level but not re-raised). Returning
    a string here makes the test fail with that string as the reason; None
    leaves the existing pass/fail decision untouched.
    """
    if not stdout:
        return None
    marker = _HANDLER_ERROR_RE.search(stdout)
    if marker is None:
        return None

    # Walk forward from the marker, collecting lines until we hit a clear
    # boundary (next pytest message, next iso-timestamped log line, end of
    # output). exc_info=True writes "Traceback (most recent call last):"
    # immediately after the message, so consecutive lines belong together.
    start = marker.start()
    tail = stdout[start:]
    # Boundary regex: a line that starts with pytest's section header
    # (=====, PASSED, FAILED) or another ISO-timestamped log line.
    boundary = re.search(
        r"\n(?:={3,}|PASSED|FAILED|\d{4}-\d{2}-\d{2}T?\d{2}:\d{2}:\d{2})",
        tail,
    )
    end = boundary.start() if boundary else len(tail)
    snippet = tail[:end].rstrip()
    if len(snippet) > _HANDLER_ERROR_MAX_LEN:
        snippet = snippet[:_HANDLER_ERROR_MAX_LEN] + "\n... [truncated]"
    return snippet


def _make_artifact_dir(item: pytest.Item) -> Path:
    # Default location: <pytest_rootpath>/.memory_test_artifacts/ — gitignored,
    # lives inside the repo so it's discoverable. Override via SODA_MEMTEST_ARTIFACTS_ROOT.
    override = os.environ.get(ARTIFACTS_ROOT_ENV)
    if override:
        root = Path(override)
    else:
        root = Path(item.config.rootpath) / ARTIFACTS_SUBDIR
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", item.nodeid).strip("_")[:120]
    # Disambiguate truncated nodeids and avoid stomping prior runs.
    short_hash = hashlib.sha1(item.nodeid.encode()).hexdigest()[:8]
    timestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    artifact_dir = root / sanitized / f"{timestamp}_{short_hash}"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    # Pre-create inner-poller output files on the host so they exist even if
    # the container OOM-kills before the inner poller writes anything. Lets us
    # distinguish "this source produced 0" from "this source vanished".
    (artifact_dir / _PEAK_FILENAME).write_text("0\n")
    (artifact_dir / "rss_timeseries.csv").write_text("timestamp_ns,rss_bytes\n")
    return artifact_dir


class _DockerStatsPoller(threading.Thread):
    """Polls `docker stats --no-stream` at ~5 Hz, tracks peak. OOM-survivable."""

    def __init__(self, cidfile: Path) -> None:
        super().__init__(daemon=True, name="memtest-stats-poller")
        self.cidfile = cidfile
        self._stop_event = threading.Event()
        self.peak_bytes = 0

    def run(self) -> None:
        # Wait briefly for the container to start and write its CID.
        cid: Optional[str] = None
        for _ in range(50):
            if self.cidfile.exists():
                try:
                    text = self.cidfile.read_text().strip()
                except OSError:
                    text = ""
                if text:
                    cid = text
                    break
            if self._stop_event.wait(0.1):
                return
        if cid is None:
            return

        interval = 1.0 / _HOST_POLL_HZ
        while not self._stop_event.is_set():
            try:
                result = subprocess.run(
                    ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", cid],
                    capture_output=True,
                    text=True,
                    timeout=2.0,
                )
            except subprocess.TimeoutExpired:
                if self._stop_event.wait(interval):
                    return
                continue

            if result.returncode != 0:
                return  # container is gone

            usage_bytes = _parse_docker_memusage(result.stdout)
            if usage_bytes > self.peak_bytes:
                self.peak_bytes = usage_bytes

            if self._stop_event.wait(interval):
                return

    def stop(self) -> None:
        self._stop_event.set()


_DOCKER_UNIT_BYTES = {
    "B": 1,
    "KiB": 1024,
    "KB": 1000,
    "MiB": 1024**2,
    "MB": 1000**2,
    "GiB": 1024**3,
    "GB": 1000**3,
}
_MEMUSAGE_RE = re.compile(r"^\s*([\d.]+)\s*([KMG]i?B|B)\s*/")


def _parse_docker_memusage(stdout: str) -> int:
    """Parse first column of `docker stats --format '{{.MemUsage}}'`.

    Example outputs: `3.5MiB / 128MiB`, `0B / 0B`, `1.2GiB / 2GiB`.
    """
    match = _MEMUSAGE_RE.match(stdout)
    if not match:
        return 0
    value, unit = match.group(1), match.group(2)
    try:
        return int(float(value) * _DOCKER_UNIT_BYTES[unit])
    except (KeyError, ValueError):
        return 0


def _generate_memray_reports(image: str, artifact_dir: Path, bin_path: Path) -> dict[str, Path]:
    """Run a second container against the same image to render memray output.

    Keeps the host system memray-free — we only need docker on PATH.
    Outputs land in the artifact dir alongside the .bin file:
      - flamegraph.html (interactive HTML flamegraph)
      - summary.txt (text top-N attribution)

    Returns a dict of {label: path} for the reports that were actually produced.
    """
    artifact_str = str(artifact_dir)
    bin_in_container = str(bin_path)
    produced: dict[str, Path] = {}

    def _run_memray_subcmd(label: str, subcmd: list[str], output_path: Path, capture_stdout: bool) -> None:
        cmd = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{artifact_str}:{artifact_str}",
            "-w",
            artifact_str,
            image,
            *subcmd,
        ]
        try:
            if capture_stdout:
                with open(output_path, "w") as out_file:
                    result = subprocess.run(cmd, stdout=out_file, stderr=subprocess.PIPE, text=True, timeout=120)
            else:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        except subprocess.TimeoutExpired:
            logger.warning("memray %s timed out after 120s", label)
            return
        except OSError as exc:
            logger.warning("memray %s failed to launch: %s", label, exc)
            return
        if result.returncode != 0:
            logger.warning("memray %s exited %s: %s", label, result.returncode, (result.stderr or "")[:500])
            return
        if output_path.exists() and output_path.stat().st_size > 0:
            produced[label] = output_path

    flamegraph_path = artifact_dir / "flamegraph.html"
    _run_memray_subcmd(
        "flamegraph",
        ["memray", "flamegraph", "--force", "-o", str(flamegraph_path), bin_in_container],
        flamegraph_path,
        capture_stdout=False,
    )
    summary_path = artifact_dir / "summary.txt"
    _run_memray_subcmd(
        "summary",
        ["memray", "summary", bin_in_container],
        summary_path,
        capture_stdout=True,
    )
    return produced


def _format_failure(rc: int, image: str, stderr: str) -> str:
    if rc == 125:
        head = (
            f"Docker daemon rejected the run (exit 125). Likely causes: image "
            f"'{image}' is not built (run "
            f"`make -C soda-core/soda-tests/memory_container image-build`), "
            f"or the Docker daemon is not running."
        )
    elif rc == 127:
        head = "`docker` not found on PATH (exit 127). Install Docker / Docker Desktop and ensure it's on PATH."
    else:
        head = f"Container pytest exited {rc}."

    if stderr:
        tail = stderr.strip()
        if len(tail) > 2000:
            tail = "…(truncated)…\n" + tail[-2000:]
        return f"{head}\n\nstderr:\n{tail}"
    return head


def _kill_container(cidfile: Path) -> None:
    """Best-effort docker kill of the test container via its cidfile.
    Used when the container exceeds SODA_MEMTEST_CONTAINER_TIMEOUT — the
    --rm flag then cleans the container up after the kill."""
    try:
        cid = cidfile.read_text().strip()
        if cid:
            subprocess.run(["docker", "kill", cid], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as e:
        logging.getLogger(__name__).debug(f"Best-effort container kill failed: {e}")


_INNER_SUMMARY_RE = re.compile(r"=+ (?P<body>[^=]+?) in [0-9.]+s")


def _inner_run_skipped(stdout: str) -> bool:
    """True when the inner pytest's final summary line reports ONLY skips
    (e.g. '==== 1 skipped in 0.01s ===='). Runtime pytest.skip() calls exit
    with rc=0 and are invisible to the static-marker check that runs before
    docker dispatch."""
    for line in reversed(stdout.splitlines()):
        match = _INNER_SUMMARY_RE.search(line)
        if match:
            body = match.group("body")
            return "skipped" in body and "passed" not in body and "failed" not in body and "error" not in body
    return False


def _emit_skipped(item: pytest.Item, message: str) -> None:
    item.ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
    longrepr = (str(item.fspath), 0, message)
    item.ihook.pytest_runtest_logreport(report=_build_report(item, when="setup", outcome="skipped", longrepr=longrepr))
    item.ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)


def _emit_failed(item: pytest.Item, when: str, longrepr: str) -> None:
    item.ihook.pytest_runtest_logstart(nodeid=item.nodeid, location=item.location)
    item.ihook.pytest_runtest_logreport(report=_build_report(item, when=when, outcome="failed", longrepr=longrepr))
    item.ihook.pytest_runtest_logfinish(nodeid=item.nodeid, location=item.location)


def _build_report(
    item: pytest.Item,
    when: str,
    outcome: str,
    longrepr=None,
    duration: float = 0.0,
) -> TestReport:
    return TestReport(
        nodeid=item.nodeid,
        location=item.location,
        keywords=dict.fromkeys(item.keywords, 1),
        outcome=outcome,
        longrepr=longrepr,
        when=when,
        sections=[],
        duration=duration,
        user_properties=[],
    )


def _build_env_args(artifact_dir: Path) -> tuple[list[str], Optional[Path]]:
    """Returns (docker args, env-file path to delete after the run).

    Propagated env vars (including DB passwords) ride an --env-file with
    0600 permissions instead of ``-e KEY=VALUE`` argv — argv is world-visible
    in ``ps`` and ``docker inspect``. docker's env-file format cannot carry
    newlines in values; those rare entries fall back to ``-e``.
    """
    args: list[str] = []
    env_file_lines: list[str] = []
    propagated_keys: set[str] = set()
    for key, value in os.environ.items():
        if key in ENV_DENYLIST:
            continue
        if any(key.startswith(prefix) for prefix in ENV_DENYLIST_PREFIXES):
            continue
        if key.endswith("_HOST") and _LOCALHOST_RE.match(value):
            value = _LOCALHOST_RE.sub(r"host.docker.internal\2", value)
        if "\n" in value or "\n" in key:
            args.extend(["-e", f"{key}={value}"])
        else:
            env_file_lines.append(f"{key}={value}")
        propagated_keys.add(key)

    env_file_path: Optional[Path] = None
    if env_file_lines:
        fd, tmp_name = tempfile.mkstemp(prefix="soda_memtest_env_", text=True)
        with os.fdopen(fd, "w") as env_file:  # mkstemp creates it 0600
            env_file.write("\n".join(env_file_lines) + "\n")
        env_file_path = Path(tmp_name)
        args.extend(["--env-file", str(env_file_path)])

    # Special-case SQLSERVER_HOST. The SqlServer test helper defaults to
    # "localhost" when the env var is unset (see SqlServerDataSourceTestHelper).
    # On the host that's fine — SQL Server is reachable on the host's
    # localhost — but from inside the container "localhost" resolves to the
    # container itself. The general localhost-rewrite above only fires when
    # the env var IS set, so an unset SQLSERVER_HOST falls through and the
    # in-container helper picks up the wrong default. Inject the
    # docker-bridge hostname explicitly so the helper resolves the host's
    # SQL Server. Only kicks in when the user hasn't already exported the
    # var themselves (e.g. to point at a non-localhost server).
    if "SQLSERVER_HOST" not in propagated_keys:
        args.extend(["-e", "SQLSERVER_HOST=host.docker.internal"])
    # Same fall-through exists for postgres: the helper defaults
    # POSTGRES_HOST to "localhost" when unset, which inside the container
    # resolves to the container itself. Inject the docker-bridge hostname
    # so an unset var reaches the host's postgres, mirroring SQLSERVER_HOST.
    if "POSTGRES_HOST" not in propagated_keys:
        args.extend(["-e", "POSTGRES_HOST=host.docker.internal"])

    args.extend(["-e", f"{INNER_ENV_VAR}=1"])
    args.extend(["-e", f"{ARTIFACT_DIR_ENV}={artifact_dir}"])
    # Source is bind-mounted :ro — without this the import system tries to
    # write .pyc files next to the .py sources and EROFS-fails.
    args.extend(["-e", "PYTHONDONTWRITEBYTECODE=1"])
    return args, env_file_path


def _build_credential_mount_args(bind_root: str) -> list[str]:
    """Bind-mount credential paths into the container read-only.

    Sources:
      1. Env vars matching *_PATH / *_CREDENTIALS / *_KEYFILE / *_KEY_FILE / GOOGLE_APPLICATION_CREDENTIALS
         whose value is an absolute path that exists on the host.
      2. Well-known dirs: ~/.aws, ~/.config/gcloud, ~/.azure.

    Skips anything already under bind_root (already mounted RO via the broad mount)
    or duplicates. Mount target == source path so env values resolve transparently.
    """
    args: list[str] = []
    mounted: set[str] = set()
    bind_root_resolved = str(Path(bind_root).resolve())

    def _add(host_path: Path) -> None:
        try:
            resolved = host_path.resolve()
        except OSError:
            return
        resolved_str = str(resolved)
        if resolved_str in mounted:
            return
        # Already covered by the broad RO mount.
        if resolved_str == bind_root_resolved or resolved_str.startswith(bind_root_resolved + os.sep):
            return
        # Denylisted: don't auto-mount sensitive subtrees regardless of how the
        # env var was named (e.g. a `FOO_PATH=~/.ssh/id_rsa` should NOT leak).
        for forbidden in _CREDENTIAL_MOUNT_DENYLIST:
            if resolved_str == forbidden or resolved_str.startswith(forbidden + os.sep):
                logger.warning(
                    "memory_container: skipping credential auto-mount of %s — "
                    "matches denylist entry %s. Set the value manually inside "
                    "the container if you really need it.",
                    resolved_str,
                    forbidden,
                )
                return
        mounted.add(resolved_str)
        args.extend(["-v", f"{resolved_str}:{resolved_str}:ro"])

    for key, value in os.environ.items():
        if key in ENV_DENYLIST or any(key.startswith(p) for p in ENV_DENYLIST_PREFIXES):
            continue
        if key not in _CREDENTIAL_ENV_EXACT and not any(key.endswith(s) for s in _CREDENTIAL_ENV_SUFFIXES):
            continue
        if not value:
            continue
        try:
            path = Path(value)
            if not path.is_absolute() or not path.exists():
                continue
        except (OSError, ValueError):
            continue
        _add(path)

    for well_known in _WELL_KNOWN_CREDENTIAL_DIRS:
        path = Path(well_known).expanduser()
        if path.exists():
            _add(path)

    return args
