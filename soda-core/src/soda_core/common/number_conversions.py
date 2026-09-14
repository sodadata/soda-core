from __future__ import annotations

import math
from numbers import Number


def is_finite_number(value: object) -> bool:
    """True for a number that JSON, and so the Soda Cloud API, can carry.

    NaN and infinity have no JSON representation. The HTTP client refuses to serialize them, so a
    single one in a results payload loses the whole upload. ``bool`` counts as a number, as it does
    for ``isinstance(value, Number)``. A value ``math.isfinite`` cannot convert, a signalling
    ``Decimal('sNaN')`` or an int beyond float range, is not finite.
    """
    if not isinstance(value, Number):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, ValueError, OverflowError):
        return False
