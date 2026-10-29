"""Translate the fixed-width subset of regex into SQL Server LIKE terms.

T-SQL has no regex operator before SQL Server 2025. The connector used to rewrite a regex
into ``PATINDEX('%<pattern>%', …) > 0``, which is substring matching with the pattern's own
anchors passed through as literal characters -- so ``^\\d{5}$`` matched nothing and a check
that should have failed 233 rows reported a clean pass. A wrong answer that looks like a pass
is worse than an error, because nobody investigates a green check.

SQL Server's ``LIKE`` is anchored and understands character classes, so the fixed-width
subset of regex translates exactly. Anything outside it raises ``RegexNotTranslatable``
naming the construct. **The translator never approximates** -- approximating is the bug it
replaces.

Parsing uses CPython's own regex parser rather than a hand-written scanner. An earlier
hand-rolled version shipped four wrong-answer bugs: a Python-side property test missed them
because it modelled LIKE itself, so both sides were wrong together, and only a differential
test against a real SQL Server caught them. Walking the parser's AST removes that class of
mistake. The parser is the authority on what a pattern means, and ranges arrive as code
points, so expanding them is forced rather than remembered.

The parser is a private stdlib module (``sre_parse`` before 3.11, ``re._parser`` after), so
the import is guarded. If it is ever absent every pattern is refused, and the engine then
errors on a regex check -- the safe direction.

Supported
    anchors (required), literal text, ``.``,
    escapes ``\\d \\D \\w \\W \\s \\S`` and escaped metacharacters,
    character classes with ranges and negation,
    counted repetition ``{n}``, optional ``?`` on an element or group,
    alternation and grouping, and negative lookahead ``(?!a|b)`` whose branches
    share a width and whose following text is fixed-width.

Refused
    unbounded or variable repetition (``* + {n,} {n,m}``), backreferences,
    positive lookahead, lookbehind, inner anchors, and anything expanding past
    ``MAX_ALTERNATIVES`` LIKE terms.
"""

from __future__ import annotations

try:  # 3.11 renamed sre_parse to re._parser. Both are private, hence the guard.
    import re._constants as _sre_constants  # type: ignore[import-not-found]
    import re._parser as _sre_parse  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised only on Python 3.10
    try:
        import sre_constants as _sre_constants  # type: ignore[no-redef]
        import sre_parse as _sre_parse  # type: ignore[no-redef]
    except ImportError:  # pragma: no cover - a CPython with neither
        _sre_constants = None  # type: ignore[assignment]
        _sre_parse = None  # type: ignore[assignment]

# A pattern expanding past this many alternatives produces a predicate too large to be worth
# sending; refusing beats emitting a thousand-term OR.
MAX_ALTERNATIVES = 64

# A range wider than this is a character set nobody wrote on purpose.
MAX_RANGE_WIDTH = 128

# LIKE's own metacharacters, escaped by the bracket form LIKE understands.
_LIKE_META = {"%": "[%]", "_": "[_]", "[": "[[]"}

# Digits have no case, so a digit RANGE cannot be widened by collation; it stays a range,
# which keeps the generated SQL readable. Letter ranges are expanded (see _class_body).
_DIGITS = "0-9"
_UPPER = "".join(chr(c) for c in range(ord("A"), ord("Z") + 1))
_LOWER = "".join(chr(c) for c in range(ord("a"), ord("z") + 1))
# The only whitespace these rules meet in practice, and the only one a LIKE class can carry.
_SPACE = " "


class RegexNotTranslatable(Exception):
    """The pattern has no exact LIKE equivalent. Carries the construct that stopped it."""


class _Element:
    """One regex node, as the set of LIKE fragments it can match.

    ``fragments`` are alternatives. ``width`` is how many characters every fragment
    consumes, or -1 when they differ -- which is what decides whether a negative lookahead
    can be expressed.
    """

    __slots__ = ("fragments", "width", "lookahead")

    def __init__(self, fragments: list[str], width: int, lookahead: bool = False) -> None:
        self.fragments = fragments
        self.width = width
        self.lookahead = lookahead


def _op(name: str):
    """A parser opcode by name, or a unique sentinel when this Python lacks it."""
    return getattr(_sre_constants, name, object())


def _escape_literal(ch: str) -> str:
    return _LIKE_META.get(ch, ch)


def _category_chars(category) -> str:
    name = str(category)
    if name.endswith("CATEGORY_DIGIT"):
        return _DIGITS
    if name.endswith("CATEGORY_SPACE"):
        return _SPACE
    if name.endswith("CATEGORY_WORD"):
        return _DIGITS + _UPPER + _LOWER + "_"
    raise RegexNotTranslatable(f"the character category {name}")


def _negated_category_class(category) -> str:
    name = str(category)
    if name.endswith("CATEGORY_NOT_DIGIT"):
        return "[^" + _DIGITS + "]"
    if name.endswith("CATEGORY_NOT_SPACE"):
        return "[^" + _SPACE + "]"
    if name.endswith("CATEGORY_NOT_WORD"):
        return "[^" + _DIGITS + _UPPER + _LOWER + "_]"
    raise RegexNotTranslatable(f"the character category {name}")


def _class_body(members: list, negated: bool) -> str:
    """Render an IN node's members as a LIKE character class.

    Every range is expanded to explicit characters. SQL Server resolves a LIKE range by
    COLLATION SORT ORDER, not code point, and SQL_Latin1_General interleaves case
    (a A b B c C ...) -- so ``[A-Z]`` also matches lowercase even under a case-sensitive
    collation. Measured on SQL Server 2022: ``'AA' LIKE '[a-z][a-z]'`` is true, and
    ``'b9c' LIKE '[A-Z][0-9][A-Z]'`` is true. Expanding removes the question, including for
    partial ranges such as the Canadian postcode alphabet's ``J-N``.
    """
    out: list[str] = []
    for op, av in members:
        if op is _op("LITERAL"):
            out.append(chr(av))
        elif op is _op("RANGE"):
            lo, hi = av
            if chr(lo).isdigit() and chr(hi).isdigit():
                out.append(f"{chr(lo)}-{chr(hi)}")  # safe: digits have no case variants
                continue
            if hi - lo + 1 > MAX_RANGE_WIDTH:
                raise RegexNotTranslatable(
                    f"a character range spanning more than {MAX_RANGE_WIDTH} characters"
                )
            out.append("".join(chr(c) for c in range(lo, hi + 1)))
        elif op is _op("CATEGORY"):
            name = str(av)
            if "NOT_" in name:
                # [\D] inside a larger class cannot be expressed: LIKE has no union of a
                # negated set with a positive one.
                raise RegexNotTranslatable(
                    f"the negated category {name} inside a character class"
                )
            out.append(_category_chars(av))
        else:
            raise RegexNotTranslatable(f"the character-class element {op}")
    body = "".join(out)
    if not body:
        raise RegexNotTranslatable("an empty character class")
    if "]" in body:
        # LIKE has no escape for ']' inside a class: it would close the class early.
        raise RegexNotTranslatable("a ']' inside a character class")
    if body.startswith("^") and not negated:
        raise RegexNotTranslatable("a literal '^' as the first character-class member")
    return ("[^" if negated else "[") + body + "]"


def _node(op, av) -> _Element:
    """One AST node to an element. Raises for anything with no exact LIKE equivalent."""
    if op is _op("LITERAL"):
        return _Element([_escape_literal(chr(av))], 1)
    if op is _op("NOT_LITERAL"):
        return _Element([f"[^{chr(av)}]"], 1)
    if op is _op("ANY"):
        return _Element(["_"], 1)
    if op is _op("IN"):
        members = list(av)
        negated = bool(members) and members[0][0] is _op("NEGATE")
        if negated:
            members = members[1:]
        if (
            len(members) == 1
            and members[0][0] is _op("CATEGORY")
            and "NOT_" in str(members[0][1])
            and not negated
        ):
            return _Element([_negated_category_class(members[0][1])], 1)
        return _Element([_class_body(members, negated)], 1)
    if op is _op("MAX_REPEAT") or op is _op("MIN_REPEAT"):
        lo, hi, sub = av
        maxrepeat = getattr(_sre_constants, "MAXREPEAT", None)
        if hi is maxrepeat or hi == maxrepeat:
            raise RegexNotTranslatable("unbounded repetition ('*' or '+')")
        inner = _sequence(sub)
        if lo == 0 and hi == 1:
            return _Element(inner.fragments + [""], -1)  # optional: the thing, or nothing
        if lo != hi:
            raise RegexNotTranslatable(f"the variable repetition {{{lo},{hi}}}")
        if inner.width < 0:
            raise RegexNotTranslatable(f"repetition {{{lo}}} of a variable-width element")
        combos = [""]
        for _ in range(lo):
            combos = [p + f for p in combos for f in inner.fragments]
            _guard(combos)
        return _Element(combos, inner.width * lo)
    if op is _op("SUBPATTERN"):
        return _sequence(av[-1])
    if op is _op("ATOMIC_GROUP"):
        return _sequence(av)
    if op is _op("BRANCH"):
        _, branches = av
        fragments: list[str] = []
        widths = set()
        for branch in branches:
            element = _sequence(branch)
            fragments.extend(element.fragments)
            widths.add(element.width)
        _guard(fragments)
        return _Element(fragments, widths.pop() if len(widths) == 1 else -1)
    if op is _op("ASSERT_NOT"):
        direction, sub = av
        if direction != 1:
            raise RegexNotTranslatable("a negative lookbehind")
        element = _sequence(sub)
        if element.width < 0:
            raise RegexNotTranslatable("a negative lookahead whose branches differ in width")
        return _Element(element.fragments, element.width, lookahead=True)
    if op is _op("ASSERT"):
        raise RegexNotTranslatable("a positive lookahead or lookbehind")
    if op is _op("AT"):
        raise RegexNotTranslatable(f"the anchor {av} inside the pattern body")
    if op is _op("GROUPREF") or op is _op("GROUPREF_EXISTS"):
        raise RegexNotTranslatable("a backreference")
    raise RegexNotTranslatable(f"the construct {op}")


def _guard(combos: list[str]) -> None:
    if len(combos) > MAX_ALTERNATIVES:
        raise RegexNotTranslatable(
            f"a pattern expanding to more than {MAX_ALTERNATIVES} LIKE terms"
        )


def _sequence(nodes) -> _Element:
    """Cross-product a run of nodes into whole fragments."""
    elements = [_node(op, av) for op, av in nodes]
    if any(e.lookahead for e in elements):
        raise RegexNotTranslatable("a negative lookahead nested inside a group")
    return _combine(elements)


def _combine(elements: list[_Element]) -> _Element:
    combos = [""]
    width = 0
    for element in elements:
        combos = [p + f for p in combos for f in element.fragments]
        _guard(combos)
        width = -1 if (width < 0 or element.width < 0) else width + element.width
    return _Element(combos, width)


def translate(regex_pattern: str) -> tuple[list[str], list[str]]:
    """Return (like_patterns, not_like_patterns) equivalent to an anchored regex.

    The caller builds::

        (expr LIKE p1 OR expr LIKE p2 ...) AND expr NOT LIKE n1 AND expr NOT LIKE n2 ...

    Raises RegexNotTranslatable, naming the construct, when no exact equivalent exists.
    """
    if _sre_parse is None:  # pragma: no cover
        raise RegexNotTranslatable(
            "no regex parser on this Python, so no pattern can be shown to have an exact "
            "LIKE equivalent"
        )
    if not regex_pattern:
        raise RegexNotTranslatable("an empty pattern")

    try:
        parsed = list(_sre_parse.parse(regex_pattern))
    except Exception as exc:  # an invalid regex is the caller's problem, not a translation one
        raise RegexNotTranslatable(f"a pattern the regex parser rejected: {exc}") from exc

    at = _op("AT")
    begin = getattr(_sre_constants, "AT_BEGINNING", None)
    end = getattr(_sre_constants, "AT_END", None)
    if not (
        len(parsed) >= 2
        and parsed[0][0] is at
        and parsed[0][1] is begin
        and parsed[-1][0] is at
        and parsed[-1][1] is end
    ):
        # Without both anchors the regex is a substring search, and LIKE's wildcards would
        # change which rows match in ways that depend on the pattern.
        raise RegexNotTranslatable("a pattern that is not anchored with both ^ and $")

    elements = [_node(op, av) for op, av in parsed[1:-1]]

    not_likes: list[str] = []
    for index, element in enumerate(elements):
        if not element.lookahead:
            continue
        before = _combine(elements[:index])
        after = _combine([e for e in elements[index + 1 :] if not e.lookahead])
        if after.width < 0:
            # Each prefix is already a concrete fragment, so prefixes of differing width are
            # fine; it is the text AFTER the lookahead that has to line up.
            raise RegexNotTranslatable(
                "a negative lookahead whose following text is not fixed-width"
            )
        tail = after.width - element.width
        if tail < 0:
            raise RegexNotTranslatable(
                "a negative lookahead longer than the text that follows it"
            )
        for excluded in element.fragments:
            for prefix in before.fragments:
                not_likes.append(prefix + excluded + ("_" * tail))
        _guard(not_likes)

    matched = _combine([e for e in elements if not e.lookahead])
    if not matched.fragments:
        raise RegexNotTranslatable("a pattern that matches nothing")
    return matched.fragments, not_likes
