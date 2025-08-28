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
