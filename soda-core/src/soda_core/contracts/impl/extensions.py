import logging
from importlib import import_module
from typing import Optional

from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


class Extensions:

    def __init__(self):
        self.is_available: bool = self._is_available()

    @classmethod
    def _is_available(cls) -> bool:
        method = cls._get_method(module_name="soda.failed_rows", class_name="FailedRows", method_name="save")
        if method is None:
            logger.debug("Soda Core OSS")
            return False
        else:
            logger.debug("Soda commercial library extensions installed")
            return True

    @classmethod
    def _get_method(cls, module_name: str, class_name: str, method_name: str) -> any:
        try:
            module = import_module(module_name)
            class_ = getattr(module, class_name)
            return getattr(class_, method_name)
        except Exception as e:
            # Extensions is not installed
            pass

    def _invoke(
        self,
        module_name: str,
        class_name: str,
        method_name: str,
        kwargs: Optional[dict[str, any]] = None
    ) -> any:
        if not self.is_available:
            return None
        try:
            method = self._get_method(
                module_name=module_name,
                class_name=class_name,
                method_name=method_name
            )
            if kwargs:
                return method(**kwargs if kwargs else None)
            else:
                return method()

        except Exception as e:
            # Extensions is not installed
            pass

    def register_check_types(self) -> None:
        self._invoke(
            module_name="soda.failed_rows",
            class_name="FailedRows",
            method_name="register_check_types",
        )


extensions: Extensions = Extensions()
