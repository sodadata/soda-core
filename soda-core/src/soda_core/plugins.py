import logging
from argparse import ArgumentParser
from importlib.metadata import entry_points
from typing import Protocol, TypeVar, runtime_checkable

from soda_core.cli.cli import cli_parser

logger = logging.getLogger(__name__)

T = TypeVar("T")


@runtime_checkable
class Plugin(Protocol[T]):
    """Protocol defining Soda plugin behavior"""

    @classmethod
    def setup_cli(cls, root_parser: ArgumentParser) -> None:
        """Hook to allow modifying the CLI for the plugin"""

    @classmethod
    def load(cls) -> T:
        """Loads the plugin module and returns the plugin protocol instance."""


def load_plugins():
    """
    Discover and load all data source plugins registered via entry points
    under 'soda.plugins.*' group.
    """
    try:
        eps = entry_points()
        if hasattr(eps, "select"):
            all_groups = eps.groups
            for group in all_groups:
                if group.startswith("soda.plugins."):
                    for ep in eps.select(group=group):
                        try:
                            plugin_cls = ep.load()
                            if isinstance(plugin_cls, Plugin):
                                plugin_cls.setup_cli(cli_parser)
                                plugin_cls.load()
                                logger.debug(f"Loaded plugin: {ep.name} -> {plugin_cls}")
                        except Exception as e:
                            logger.warning(f"Failed to load entry point '{ep.name}' from group '{group}': {e}")
    except Exception as e:
        logger.error(f"Failed to load Soda plugins: {e}")
