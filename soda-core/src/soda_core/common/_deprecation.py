from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def warn_deprecated(old_name: str, new_name: str, stacklevel: int = 2) -> None:
    warnings.warn(
        f"{old_name} is deprecated and will be removed in a future release; use {new_name} instead.",
        DeprecationWarning,
        stacklevel=stacklevel,
    )


def deprecated_alias(new_callable: Callable[..., Any], old_name: str, new_name: str) -> Callable[..., Any]:
    @functools.wraps(new_callable)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        warn_deprecated(old_name, new_name, stacklevel=3)
        return new_callable(*args, **kwargs)

    wrapper.__name__ = old_name
    wrapper.__qualname__ = old_name
    return wrapper


def deprecated_kwarg(
    kwargs: dict[str, Any],
    old_name: str,
    new_name: str,
    current_new_value: Any = None,
    sentinel: Any = None,
) -> Any:
    """Pop an old kwarg from kwargs, warn, and return the value to use for the new param.

    Returns the new param's effective value. Raises TypeError if both old and new are passed with
    conflicting values.
    """
    if old_name not in kwargs:
        return current_new_value
    old_value = kwargs.pop(old_name)
    warn_deprecated(old_name, new_name, stacklevel=3)
    if current_new_value not in (None, sentinel) and old_value != current_new_value:
        raise TypeError(f"Cannot pass both {old_name} and {new_name} with conflicting values; use {new_name} only.")
    return old_value
