from importlib import import_module
from typing import Callable, Optional

from soda_core.common.exceptions import ExtensionException


class Extensions:
    @classmethod
    def find_class_method(cls, module_name: str, class_name: str, method_name: str) -> Optional[Callable]:
        try:
            module = import_module(module_name)
            class_ = getattr(module, class_name)
            return getattr(class_, method_name)
        except (AttributeError, ModuleNotFoundError) as e:
            # Extension not installed
            return None
