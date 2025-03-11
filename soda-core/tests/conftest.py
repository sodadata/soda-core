import logging
import sys


def pytest_sessionstart(session) -> None:
    configure_logging()


def configure_logging():
    sys.stderr = sys.stdout
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("pyathena").setLevel(logging.WARNING)
    logging.getLogger("faker").setLevel(logging.ERROR)
    logging.getLogger("snowflake").setLevel(logging.WARNING)
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("pyhive").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.INFO)
    logging.getLogger("segment").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        # %(name)s
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
