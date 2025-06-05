from soda_core.common.logging_configuration import configure_logging
from soda_core.plugins import load_plugins

__all__ = [
    "configure_logging",
]

load_plugins()
