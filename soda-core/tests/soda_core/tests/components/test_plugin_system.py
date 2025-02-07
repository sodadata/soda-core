from importlib.metadata import entry_points

from soda_core.contracts.impl.instantiator import instantiate


def test_plugin():
    console_script_entry_points = entry_points()["soda_cli_plugins"]
    print(console_script_entry_points)

    epv = next(entry_point.value for entry_point in console_script_entry_points if entry_point.name == 'premium')

    parts = epv.split(':')
    module_name = parts[0]
    class_name = parts[1]
    plugin = instantiate(module_name=module_name, class_name=class_name, constructor_args=None)
    print(plugin)

#     @classmethod
#     def discover(cls, plugin_name: str) -> list:
#
#         plugin_registry: dict = {}
#         """Discover and register plugins via entry points."""
#         for entry_point in pkg_resources.iter_entry_points(plugin_name):
#             plugin_type, plugin_cls = entry_point.name, entry_point.load()
#             if not issubclass(plugin_cls, Plugin):
#                 raise TypeError(f"Registered plugin {plugin_cls} is not a subclass of Plugin.")
#             plugin_registry[eval(plugin_type)] = plugin_cls
#
#
# class CliPlugins:
#     CLI_PLUGINS: list[CliPlugin] = Plugin.load_plugins("soda.cli.plugin")
