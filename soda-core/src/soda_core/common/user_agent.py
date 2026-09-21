"""The User-Agent soda-core sends to Soda Cloud.

RFC 9110 section 10.1.5: a list of product tokens, most significant first. soda-core is
always the first token. Packages that ride on soda-core (soda-extensions, for example)
append their own so Soda Cloud can tell what stack a request came from.
"""

from __future__ import annotations

import re

from soda_core.__version__ import SODA_CORE_VERSION

SODA_CORE_PRODUCT = "soda-core"

# RFC 9110 "token": no whitespace, no separators.
_TOKEN = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")

_products: list[tuple[str, str]] = [(SODA_CORE_PRODUCT, SODA_CORE_VERSION)]


def register_user_agent_product(name: str, version: str) -> None:
    """Appends a product token to the User-Agent. Registering the same product twice replaces its
    version, so re-importing a package does not repeat the token."""
    if not _TOKEN.match(name) or not _TOKEN.match(version):
        raise ValueError(f"Invalid User-Agent product token: {name}/{version}")
    for i, (existing_name, _) in enumerate(_products):
        if existing_name == name:
            _products[i] = (name, version)
            return
    _products.append((name, version))


def user_agent() -> str:
    return " ".join(f"{name}/{version}" for name, version in _products)
