from soda_core.common.logging_configuration import configure_logging


def pytest_sessionstart(session) -> None:
    configure_logging(verbose=True)
