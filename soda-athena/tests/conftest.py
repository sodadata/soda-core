from soda_core.common.logging_configuration import configure_logging

# Register the snapshot pytest plugin so adapter tests get the
# rerun-on-mismatch behaviour and the RERAN / fallback reporting.
pytest_plugins = ["helpers.snapshot_pytest_plugin"]


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
