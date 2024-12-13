from importlib import import_module


def instantiate(module_name: str, class_name: str, constructor_args) -> object:
    module = import_module(module_name)
    class_ = getattr(module, class_name)
    return class_(constructor_args)
