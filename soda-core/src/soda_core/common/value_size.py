"""Copy-free, bounded estimation of a Python value's in-memory size.

``sys.getsizeof`` alone reports only the container header for dicts and
lists — a 10 MB parsed jsonb document counts as a few hundred bytes, which
lets fat container values slip past every byte budget (read-side fetch
batching, DWH buffer caps, insert routing). Serializing to measure
(``json.dumps`` / ``str.encode``) would copy the whole payload, which is
exactly the allocation the byte budgets exist to avoid.

``estimate_value_size`` walks the structure recursively instead, bounded
by a node budget: when the budget runs out mid-container, the visited
sample is extrapolated by element count, so a million-element list costs
``NODE_BUDGET`` getsizeof calls, not a million — and still estimates near
its true size when elements are similar (the common shape for query
results). No copies, no serialization.
"""

from __future__ import annotations

import sys

NODE_BUDGET_DEFAULT: int = 1000
# Recursion guard for adversarial nesting; the node budget governs cost.
_MAX_DEPTH: int = 32

_NONE_SIZE: int = 16  # pointer size


def estimate_value_size(value, node_budget: int = NODE_BUDGET_DEFAULT) -> int:
    """Estimated in-memory bytes of ``value``, including nested containers."""
    remaining = [node_budget]
    return _walk(value, remaining, _MAX_DEPTH)


def _walk(value, remaining: list[int], depth: int) -> int:
    if value is None:
        return _NONE_SIZE
    if isinstance(value, memoryview):
        # getsizeof reports only the view header, not the buffer it points at.
        # A large bytea read as a memoryview would otherwise count as ~tiny and
        # slip past every byte budget — count the referenced bytes instead.
        # (bytearray/bytes own their buffer, so getsizeof is already exact.)
        return sys.getsizeof(value) + value.nbytes
    size: int = sys.getsizeof(value)
    if isinstance(value, dict):
        n_children = 2 * len(value)
        children = (element for pair in value.items() for element in pair)
    elif isinstance(value, (list, tuple, set, frozenset)):
        n_children = len(value)
        children = iter(value)
    else:
        return size  # leaf — getsizeof is exact for str/bytes/numbers/Decimal
    if n_children == 0 or depth <= 0 or remaining[0] <= 0:
        return size
    visited = 0
    subtotal = 0
    for child in children:
        if remaining[0] <= 0:
            break
        remaining[0] -= 1
        subtotal += _walk(child, remaining, depth - 1)
        visited += 1
    if visited < n_children:
        # Budget ran out mid-container: extrapolate the unvisited tail from
        # the visited sample (uniform-element assumption).
        subtotal = subtotal * n_children // max(visited, 1)
    return size + subtotal
