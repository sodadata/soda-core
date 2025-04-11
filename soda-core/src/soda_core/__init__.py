"""
Soda Core - A data quality and testing framework
"""

import importlib
import importlib.metadata


def load_plugins() -> None:
    """Load all registered Soda plugins."""
    for entry_point in importlib.metadata.entry_points(group="soda.plugins"):
        try:
            importlib.import_module(entry_point.value)
        except ImportError as e:
            print(f"Could not load plugin '{entry_point.name}': {e}")


# Load plugins when the package is imported
load_plugins()
