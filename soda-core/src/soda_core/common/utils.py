from importlib.util import find_spec

is_tabulate_available: bool = bool(find_spec(name="tabulate"))
