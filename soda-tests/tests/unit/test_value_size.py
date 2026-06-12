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
