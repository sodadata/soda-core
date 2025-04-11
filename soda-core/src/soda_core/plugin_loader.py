import logging
from importlib.metadata import entry_points

logger = logging.getLogger(__name__)


def load_data_source_plugins():
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
                            cls = ep.load()
                            logger.debug(f"Loaded plugin: {ep.name} -> {cls}")
                        except Exception as e:
                            logger.warning(f"Failed to load entry point '{ep.name}' from group '{group}': {e}")
    except Exception as e:
        logger.error(f"Failed to load Soda plugins: {e}")
