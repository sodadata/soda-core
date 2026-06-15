"""Unit tests for soda_core.common.value_size.estimate_value_size.

The walker must catch fat payloads inside parsed container values (jsonb
dicts/lists) without copying or serializing them — shallow getsizeof
counted a 10 MB document as a few hundred bytes, defeating every byte
budget downstream.
"""

from __future__ import annotations

import sys

from soda_core.common.value_size import NODE_BUDGET_DEFAULT, estimate_value_size

_MB = 1_000_000


class TestLeaves:
    def test_scalar_leaves_match_getsizeof(self):
        for value in ("x" * 1000, b"y" * 1000, 42, 3.14, True):
            assert estimate_value_size(value) == sys.getsizeof(value)

    def test_none_is_pointer_size(self):
        assert estimate_value_size(None) == 16


class TestNestedContainers:
    def test_fat_string_in_list_counted(self):
        assert estimate_value_size(["x" * _MB]) > _MB

    def test_dict_counts_keys_and_values(self):
        assert estimate_value_size({"k" * _MB: "v" * _MB}) > 2 * _MB

    def test_deeply_nested_payload_counted(self):
        # The old depth-2 cutoff returned header-only here; the budgeted
        # walk follows realistic nesting all the way to the fat leaf.
        value = {"data": {"items": [{"payload": ["x" * _MB]}]}}
        assert estimate_value_size(value) > _MB

    def test_ten_levels_deep_counted(self):
        value = ["x" * _MB]
        for _ in range(9):
            value = [value]
        assert estimate_value_size(value) > _MB

    def test_adversarial_depth_does_not_recurse_forever(self):
        value = []
        for _ in range(10_000):
            value = [value]
        # Must terminate quickly (depth guard) and return something positive.
        assert estimate_value_size(value) > 0


class TestBudgetAndExtrapolation:
    def test_uniform_list_extrapolates_close_to_true_size(self):
        # 100k equal-sized strings, budget 1000: the walker visits ~1% and
        # extrapolates. True content is ~100k * (getsizeof('x'*100)).
        element = "x" * 100
        true_content = 100_000 * sys.getsizeof(element)
        value = [element] * 100_000
        estimate = estimate_value_size(value)
        assert estimate > 0.9 * true_content
        assert estimate < 1.5 * (true_content + sys.getsizeof(value))

    def test_budget_bounds_work_not_result(self):
        # A huge list costs at most ~NODE_BUDGET sizeof calls but still
        # estimates near its true size — that's the whole point.
        big = ["payload"] * (NODE_BUDGET_DEFAULT * 100)
        assert estimate_value_size(big) > NODE_BUDGET_DEFAULT * 50  # way past the visited sample

    def test_empty_containers(self):
        assert estimate_value_size([]) == sys.getsizeof([])
        assert estimate_value_size({}) == sys.getsizeof({})

    def test_custom_node_budget_extrapolates_from_small_sample(self):
        # node_budget is a real knob; a small budget still estimates near the
        # true size for uniform elements by extrapolating the visited sample.
        element = "x" * 1000
        value = [element] * 1000
        true_content = 1000 * sys.getsizeof(element)
        estimate = estimate_value_size(value, node_budget=10)
        assert 0.8 * true_content < estimate < 1.5 * (true_content + sys.getsizeof(value))


class TestSkewedExtrapolation:
    """Extrapolation assumes uniform elements. These pin the known skew
    behaviour: it is exact-ish for uniform data, can UNDER-count a fat tail
    that sits beyond the node budget, and over-counts (safe) when the fat
    elements fall within the visited sample. Downstream this is bounded —
    the read-side fetch growth ramp caps a bad estimate to one batch and the
    write side re-checks against the dialect max — so we characterise rather
    than pay for exact measurement."""

    def test_fat_tail_beyond_budget_is_undercounted(self):
        thin = ["x"] * 2000  # cheap prefix fills the node budget
        fat = ["y" * _MB] * 10  # 10 MB tail, never visited
        value = thin + fat
        # Known limitation: the fat tail alone is ~10 MB but the estimate,
        # extrapolated from the thin prefix, stays far below it.
        assert estimate_value_size(value) < 10 * _MB

    def test_fat_prefix_within_budget_is_counted(self):
        fat = ["y" * _MB] * 10  # visited first — counted in full
        thin = ["x"] * 2000
        value = fat + thin
        assert estimate_value_size(value) > 10 * _MB


class TestExoticLeaves:
    def test_memoryview_counts_referenced_buffer(self):
        # Regression: a memoryview's getsizeof is just the view header; the
        # walker must count the buffer it references (large bytea shape).
        buffer = b"z" * _MB
        assert estimate_value_size(memoryview(buffer)) > _MB

    def test_bytearray_counts_its_buffer(self):
        # bytearray owns its buffer, so plain getsizeof already covers it.
        assert estimate_value_size(bytearray(_MB)) > _MB

    def test_tuple_set_frozenset_walked_like_list(self):
        assert estimate_value_size(("x" * _MB,)) > _MB
        assert estimate_value_size({"x" * _MB}) > _MB
        assert estimate_value_size(frozenset(("x" * _MB,))) > _MB

    def test_decimal_and_datetime_are_exact_leaves(self):
        from datetime import datetime
        from decimal import Decimal

        # Small fixed-size objects: no recursion, getsizeof is exact.
        dec = Decimal("3.14159")
        assert estimate_value_size(dec) == sys.getsizeof(dec)
        dt = datetime(2026, 6, 15, 12, 30, 0)
        assert estimate_value_size(dt) == sys.getsizeof(dt)
