from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

# Sentinel used to distinguish "caller did not provide the new param" from "caller passed None/False/0".
# Without this, callsites with a concrete default (e.g. `use_runner: bool = False`) would always
# look "explicitly set" to deprecated_kwarg and trip the conflict check.
_UNSET: Any = object()


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
    current_new_value: Any = _UNSET,
) -> Any:
    """Pop an old kwarg from kwargs, warn, and return the value to use for the new param.

    Use the ``Optional[T] = None`` + normalize-at-bottom pattern at the callsite so the new
    param defaults to ``None`` when the caller didn't supply it. Both ``_UNSET`` and ``None``
    are treated as "new param not supplied" by this helper; that means callsites can simply
    forward the value of the new param without having to detect whether it was explicit.

    Returns the effective value for the new param. Raises TypeError only when the caller
    passed both the old and the new kwarg with conflicting non-None values.
    """
    if old_name not in kwargs:
        return current_new_value
    old_value = kwargs.pop(old_name)
    warn_deprecated(old_name, new_name, stacklevel=3)
    if current_new_value is _UNSET or current_new_value is None or current_new_value == old_value:
        return old_value
    raise TypeError(f"Cannot pass both {old_name} and {new_name} with conflicting values; use {new_name} only.")
