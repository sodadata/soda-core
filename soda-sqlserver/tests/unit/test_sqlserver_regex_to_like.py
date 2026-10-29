"""The regex→LIKE translation, and what it refuses.

Every accepted pattern here was checked against the real engines before being written down:
the generated predicate was run on SQL Server 2022 over a 6,716-row SAP vendor address table
and compared with the same rule evaluated as a regex on Postgres. All ten country patterns
agreed exactly, 589 failing rows on both. The shipped PATINDEX path returned 0.

The refusals matter as much as the translations. The bug this replaces was not "regex is
unsupported" — it was that an unsupported pattern produced a confident wrong answer. So the
negative cases assert that we raise, and that the message names the construct.
"""

from __future__ import annotations

import pytest
from soda_sqlserver.common.data_sources.regex_to_like import MAX_ALTERNATIVES, RegexNotTranslatable, translate

# The ten country ZIP patterns from IATA's SAP T005, with the counts each produced on both
# engines. Kept verbatim: they are the evidence that the subset is the right subset.
REAL_WORLD = [
    (r"^\d{4}$", 1, 0),                                            # AU, ZA
    (r"^\d{5}-\d{3}$", 1, 0),                                      # BR
    (r"^[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z][ ]?\d[ABCEGHJ-NPRSTV-Z]\d$", 2, 0),  # CA
    (r"^(CH-)?[1-9]\d{3}$", 2, 0),                                 # CH
    (r"^(D-)?\d{5}$", 2, 0),                                       # DE
    (r"^[1-9][0-9]{5}$", 1, 0),                                    # IN
    (r"^(0[1-9]\d{3}|[1-9]\d{4})$", 2, 0),                         # MX
    (r"^[1-9][0-9]{3}\s?(?!SA|SD|SS)[A-Z]{2}$", 2, 2),             # NL, the lookahead
    (r"^\d{5}(?:[-\s]?\d{4})?$", 3, 0),                            # US, ZIP+4
]


@pytest.mark.parametrize("pattern,n_like,n_not_like", REAL_WORLD)
def test_country_patterns_translate(pattern: str, n_like: int, n_not_like: int) -> None:
    likes, not_likes = translate(pattern)
    assert len(likes) == n_like
    assert len(not_likes) == n_not_like


def test_digits_become_a_character_class() -> None:
    likes, not_likes = translate(r"^\d{4}$")
    assert likes == ["[0-9][0-9][0-9][0-9]"]
    assert not_likes == []


def test_optional_prefix_becomes_two_alternatives() -> None:
    likes, _ = translate(r"^(D-)?\d{5}$")
    assert sorted(likes) == sorted(
        ["D-[0-9][0-9][0-9][0-9][0-9]", "[0-9][0-9][0-9][0-9][0-9]"]
    )


UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def test_negative_lookahead_becomes_not_like() -> None:
    """The Dutch rule: four digits, optional space, two letters, but not SA/SD/SS."""
    likes, not_likes = translate(r"^[1-9][0-9]{3}\s?(?!SA|SD|SS)[A-Z]{2}$")
    assert f"[1-9][0-9][0-9][0-9][{UPPER}][{UPPER}]" in likes
    assert f"[1-9][0-9][0-9][0-9][ ][{UPPER}][{UPPER}]" in likes
    # The parser factors SA|SD|SS into S[ADS], so one exclusion per prefix variant rather
    # than one per excluded pair.
    assert "[1-9][0-9][0-9][0-9]S[ADS]" in not_likes
    assert "[1-9][0-9][0-9][0-9][ ]S[ADS]" in not_likes
    assert len(not_likes) == 2


def test_letter_ranges_are_expanded_not_passed_through() -> None:
    """SQL Server resolves a LIKE range by collation sort order, which interleaves case.

    Measured on SQL Server 2022: 'AA' LIKE '[a-z][a-z]' is TRUE and 'b9c' LIKE
    '[A-Z][0-9][A-Z]' is TRUE, even under a case-sensitive collation -- the collation
    governs comparison, not what a range spans. Four patterns disagreed with the regex
    before ranges were expanded explicitly.
    """
    likes, _ = translate(r"^[a-z]{2}$")
    assert likes == ["[abcdefghijklmnopqrstuvwxyz][abcdefghijklmnopqrstuvwxyz]"]
    likes, _ = translate(r"^[A-Z]$")
    assert likes == [f"[{UPPER}]"]
    # a partial range, as in the Canadian postcode alphabet
    likes, _ = translate(r"^[J-N]$")
    assert likes == ["[JKLMN]"]
    # digit ranges are unaffected by collation and stay as they are
    likes, _ = translate(r"^[0-9]$")
    assert likes == ["[0-9]"]


def test_mixed_case_range_is_refused() -> None:
    """[A-z] spans A..z, which includes '[', ']' and '^' -- LIKE cannot carry ']' in a class."""
    with pytest.raises(RegexNotTranslatable):
        translate(r"^[A-z]$")


def test_escape_inside_a_character_class_is_expanded() -> None:
    """`[-\\s]` is the US ZIP+4 separator and has an exact character-set equivalent."""
    likes, _ = translate(r"^\d{5}(?:[-\s]?\d{4})?$")
    assert any("[- ]" in p for p in likes)
    assert "[0-9][0-9][0-9][0-9][0-9]" in likes  # the bare five-digit form survives


def test_like_metacharacters_in_the_pattern_are_escaped() -> None:
    """A literal % or _ from the regex must not become a LIKE wildcard."""
    likes, _ = translate(r"^100%$")
    assert likes == ["100[%]"]
    likes, _ = translate(r"^a_b$")
    assert likes == ["a[_]b"]


@pytest.mark.parametrize(
    "pattern,expected_fragment",
    [
        (r"^\d+$", "unbounded repetition"),
        (r"^\d*$", "unbounded repetition"),
        (r"^\d{2,5}$", "variable repetition"),
        (r"^(a)\1$", "backreference"),
        (r"^(?=foo)\d{3}$", "positive lookahead"),
        (r"^(?<=x)\d{3}$", "lookbehind"),
        (r"\d{4}", "not anchored"),
        (r"^\d{4}", "not anchored"),
        (r"^(?!ab)\d+$", "unbounded repetition"),
    ],
)
def test_untranslatable_patterns_raise_and_name_the_construct(
    pattern: str, expected_fragment: str
) -> None:
    with pytest.raises(RegexNotTranslatable) as exc:
        translate(pattern)
    assert expected_fragment in str(exc.value)


def test_explosive_pattern_is_refused_rather_than_rendered() -> None:
    """Seven independent optionals is 128 alternatives; refusing beats a 128-term OR."""
    with pytest.raises(RegexNotTranslatable) as exc:
        translate("^" + ("a?" * 7) + "$")
    assert str(MAX_ALTERNATIVES) in str(exc.value)


def test_a_lookahead_whose_tail_is_variable_is_refused() -> None:
    with pytest.raises(RegexNotTranslatable) as exc:
        translate(r"^\d(?!ab)[A-Z]?[A-Z]?$")
    assert "lookahead" in str(exc.value)
