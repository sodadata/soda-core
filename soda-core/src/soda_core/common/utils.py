from __future__ import annotations

from typing import Dict, List, Union


def pluralize(count, singular, plural=None):
    if count == 1:
        return f"{singular}"
    return f"{plural or singular + 's'}"


def format_items(items: Union[List[str], Dict[str, str]], verbose: bool = False, max_width: int = 80) -> str:
    # Normalize to list of strings
    if isinstance(items, dict):
        item_list = [f"{k}: {v}" for k, v in items.items()]
    else:
        item_list = list(items)

    if verbose:
        return "\n".join(f"- {item}" for item in item_list)
    else:
        line = ", ".join(item_list)
        if len(line) > max_width:
            return line[: max_width - 3] + "..."
        return line


def to_camel_case(snake_str: str) -> str:
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


# Taken from distutils.util for compatibility reasons. Removed in Python 3.12
def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "off", "0"):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))
