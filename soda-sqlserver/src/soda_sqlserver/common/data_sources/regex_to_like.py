"""Translate the fixed-width subset of regex into SQL Server LIKE terms.

T-SQL has no regex operator before SQL Server 2025. The connector used to rewrite a regex
into ``PATINDEX('%<pattern>%', …) > 0``, which is substring matching with the pattern's own
anchors left in as literal characters -- so ``^\\d{5}$`` matched nothing at all, and a check
that should have failed 233 rows reported a clean pass. A wrong answer that looks like a pass
is worse than an error, because nobody investigates a green check.

SQL Server's ``LIKE`` is more capable than it looks: it supports character classes and ranges
(``[0-9]``, ``[A-Z]``, ``[^...]``), single-character wildcards, and it anchors by default.
That covers the shape most format rules actually take -- postcodes, identifiers, codes --
which are fixed-width alternations rather than open-ended patterns.

So: translate what can be translated exactly, and raise on everything else. The translator
never approximates. Every pattern it accepts produces a predicate that agrees with the regex
row for row; every pattern it cannot express raises ``RegexNotTranslatable`` naming the
construct, and the caller turns that into a check error.

Supported
    anchors (required), literal text, ``.``,
    escapes ``\\d \\D \\w \\W \\s \\S`` and escaped metacharacters,
    character classes including ranges and negation,
    counted repetition ``{n}``,
    optional ``?`` on a single element or a group,
    alternation and grouping ``(a|b)``, ``(?:a|b)``,
    negative lookahead ``(?!a|b)`` where every branch is the same width.

Refused
    unbounded repetition ``* + {n,} {n,m}``, backreferences, positive lookahead,
    lookbehind, anchors inside the body, and anything whose expansion exceeds
    ``MAX_ALTERNATIVES`` LIKE terms.
"""

from __future__ import annotations

# A pattern expanding past this many alternatives produces a predicate too large to be worth
# sending; refusing is better than emitting a thousand-term OR.
MAX_ALTERNATIVES = 64

# LIKE's own metacharacters. A literal from the regex must not be read as a wildcard.
_LIKE_META = {"%": "[%]", "_": "[_]", "[": "[[]"}

_CLASS_FOR_ESCAPE = {
    "d": "[0-9]",
    "D": "[^0-9]",
    "w": "[0-9" + "".join(chr(c) for c in range(65, 91)) + "".join(chr(c) for c in range(97, 123)) + "_]",
    "W": "[^0-9" + "".join(chr(c) for c in range(65, 91)) + "".join(chr(c) for c in range(97, 123)) + "_]",
    "s": " ",   # the only whitespace that occurs in practice, and the only one LIKE can match
    "S": "[^ ]",
}


# Inside a class LIKE has no escapes, so these are spliced in as raw set members.
_IN_CLASS_FOR_ESCAPE = {
    "d": "0-9",
    "w": "0-9" + "".join(chr(c) for c in range(65, 91)) + "".join(chr(c) for c in range(97, 123)) + "_",
    "s": " ",
}


class RegexNotTranslatable(Exception):
    """The pattern has no exact LIKE equivalent. Carries the construct that stopped it."""


class _Element:
    """One regex element, as the set of fixed-width LIKE fragments it can match.

    `fragments` are alternatives: an element matching one of several things contributes
    several. `width` is the number of characters every fragment consumes, which is what
    makes a negative lookahead translatable.
    """

    __slots__ = ("fragments", "width", "lookahead")

    def __init__(self, fragments: list[str], width: int, lookahead: bool = False) -> None:
        self.fragments = fragments
        self.width = width
        self.lookahead = lookahead


def _literal(ch: str) -> str:
    return _LIKE_META.get(ch, ch)


def _expand_alpha_ranges(class_body: str) -> str:
    """Rewrite letter ranges inside a class as explicit characters.

    SQL Server resolves a LIKE range by COLLATION SORT ORDER, not by code point, and
    SQL_Latin1_General interleaves case -- a A b B c C ... So `[A-Z]` also matches
    lowercase, and `[a-z]` also matches uppercase, EVEN under a case-sensitive collation:
    the collation governs comparison, not what the range spans. Measured on SQL Server
    2022: `'AA' LIKE '[a-z][a-z]'` is true, and `'b9c' LIKE '[A-Z][0-9][A-Z]'` is true.

    Digit ranges are unaffected, so they are left alone. Mixed ranges (`A-z`) have no
    sensible expansion and are refused by the caller.
    """
    out, i = [], 0
    while i < len(class_body):
        if (
            i + 2 < len(class_body)
            and class_body[i + 1] == "-"
            and class_body[i].isalpha()
            and class_body[i + 2].isalpha()
        ):
            lo, hi = class_body[i], class_body[i + 2]
            if lo.islower() != hi.islower():
                raise RegexNotTranslatable(f"the mixed-case range {lo}-{hi}")
            if ord(lo) > ord(hi):
                raise RegexNotTranslatable(f"the reversed range {lo}-{hi}")
            out.append("".join(chr(c) for c in range(ord(lo), ord(hi) + 1)))
            i += 3
        else:
            out.append(class_body[i])
            i += 1
    return "".join(out)


class _Parser:
    def __init__(self, pattern: str) -> None:
        self.p = pattern
        self.i = 0

    # -- helpers ---------------------------------------------------------------

    def _peek(self) -> str | None:
        return self.p[self.i] if self.i < len(self.p) else None

    def _refuse(self, what: str) -> None:
        raise RegexNotTranslatable(what)

    # -- element parsing -------------------------------------------------------

    def _read_class(self) -> _Element:
        """A bracketed character class. LIKE understands these almost verbatim."""
        start = self.i
        self.i += 1  # consume '['
        if self._peek() == "^":
            self.i += 1
        if self._peek() == "]":  # a literal ] as the first member
            self.i += 1
        while self._peek() is not None and self._peek() != "]":
            if self._peek() == "\\":
                self.i += 1
            self.i += 1
        if self._peek() != "]":
            self._refuse("an unterminated character class")
        self.i += 1
        body = self.p[start : self.i]
        if "\\" in body:
            # LIKE has no escapes inside a class, so expand the ones with an exact
            # character-set equivalent and refuse the rest. `[-\s]` is the common case.
            inner, out = body[1:-1], []
            j = 0
            while j < len(inner):
                if inner[j] == "\\":
                    nxt = inner[j + 1] if j + 1 < len(inner) else ""
                    expansion = _IN_CLASS_FOR_ESCAPE.get(nxt)
                    if expansion is None:
                        self._refuse(f"the escape \\{nxt} inside the character class {body!r}")
                    out.append(expansion)
                    j += 2
                else:
                    out.append(inner[j])
                    j += 1
            body = "[" + "".join(out) + "]"
        negated = body.startswith("[^")
        inner = body[2:-1] if negated else body[1:-1]
        body = ("[^" if negated else "[") + _expand_alpha_ranges(inner) + "]"
        return _Element([body], 1)

    def _read_group(self) -> _Element:
        """A (...) group: alternation, non-capturing, or a negative lookahead."""
        self.i += 1  # consume '('
        lookahead = False
        if self.p.startswith("?:", self.i):
            self.i += 2
        elif self.p.startswith("?!", self.i):
            self.i += 2
            lookahead = True
        elif self.p.startswith("?=", self.i):
            self._refuse("a positive lookahead (?=...)")
        elif self.p.startswith("?<", self.i):
            self._refuse("a lookbehind (?<...)")

        depth = 1
        start = self.i
        while self.i < len(self.p) and depth:
            c = self.p[self.i]
            if c == "\\":
                self.i += 1
            elif c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    break
            self.i += 1
        if depth:
            self._refuse("an unbalanced group")
        body = self.p[start : self.i]
        self.i += 1  # consume ')'

        branches = [_translate_sequence(b) for b in _split_alternation(body)]
        widths = {w for _, w in branches}
        if len(widths) != 1:
            # Branches of different widths are fine for a normal group (they just produce
            # alternatives) but not for a lookahead, where the exclusion has to line up.
            if lookahead:
                self._refuse("a negative lookahead whose branches differ in width")
        fragments: list[str] = []
        for frags, _ in branches:
            fragments.extend(frags)

        if lookahead:
            return _Element(fragments, next(iter(widths)), lookahead=True)
        if len(widths) != 1:
            # Normal alternation with mixed widths: still translatable, but the element has
            # no single width, so nothing after it may be a lookahead. Track the max and let
            # a later lookahead refuse on its own terms.
            el = _Element(fragments, -1)
            return el
        return _Element(fragments, next(iter(widths)))

    def _read_element(self) -> _Element:
        c = self._peek()
        if c is None:
            raise StopIteration
        if c in "^$":
            self._refuse(f"the anchor {c!r} inside the pattern body")
        if c == "[":
            return self._read_class()
        if c == "(":
            return self._read_group()
        if c == ".":
            self.i += 1
            return _Element(["_"], 1)
        if c == "\\":
            self.i += 1
            nxt = self._peek()
            if nxt is None:
                self._refuse("a trailing backslash")
            self.i += 1
            if nxt.isdigit():
                self._refuse(f"the backreference \\{nxt}")
            if nxt in _CLASS_FOR_ESCAPE:
                return _Element([_CLASS_FOR_ESCAPE[nxt]], 1)
            return _Element([_literal(nxt)], 1)
        if c in "*+":
            self._refuse(f"unbounded repetition {c!r}")
        self.i += 1
        return _Element([_literal(c)], 1)

    def _read_quantifier(self, element: _Element) -> list[_Element]:
        """Apply any quantifier following `element`, returning the elements it expands to."""
        c = self._peek()
        if c is None:
            return [element]
        if c == "?":
            self.i += 1
            if self._peek() == "?":
                self.i += 1  # lazy ?? matches the same set as ?
            # Optional: the element, or nothing.
            return [_Element(element.fragments + [""], -1)]
        if c in "*+":
            self.i += 1
            self._refuse(f"unbounded repetition {c!r}")
        if c == "{":
            close = self.p.find("}", self.i)
            if close == -1:
                self._refuse("an unterminated {...} quantifier")
            spec = self.p[self.i + 1 : close]
            self.i = close + 1
            if not spec.isdigit():
                self._refuse(f"the variable repetition {{{spec}}}")
            n = int(spec)
            if n == 0:
                return []
            if element.width != 1:
                self._refuse(f"repetition {{{n}}} of a multi-width element")
            return [_Element(list(element.fragments), 1) for _ in range(n)]
        return [element]

    def parse(self) -> list[_Element]:
        out: list[_Element] = []
        while self.i < len(self.p):
            try:
                el = self._read_element()
            except StopIteration:
                break
            out.extend(self._read_quantifier(el))
        return out


def _split_alternation(body: str) -> list[str]:
    """Split on top-level `|`, ignoring those inside groups or classes."""
    parts, depth, in_class, start = [], 0, False, 0
    i = 0
    while i < len(body):
        c = body[i]
        if c == "\\":
            i += 2
            continue
        if in_class:
            if c == "]":
                in_class = False
        elif c == "[":
            in_class = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "|" and depth == 0:
            parts.append(body[start:i])
            start = i + 1
        i += 1
    parts.append(body[start:])
    return parts


def _combine(elements: list[_Element]) -> tuple[list[str], int]:
    """Cross-product the elements into whole LIKE patterns, plus their common width."""
    combos = [""]
    for el in elements:
        if el.lookahead:
            continue  # handled by the caller; contributes nothing to the match itself
        nxt: list[str] = []
        for prefix in combos:
            for frag in el.fragments:
                nxt.append(prefix + frag)
        if len(nxt) > MAX_ALTERNATIVES:
            raise RegexNotTranslatable(
                f"a pattern expanding to more than {MAX_ALTERNATIVES} LIKE terms"
            )
        combos = nxt
    widths = {_like_width(c) for c in combos}
    return combos, (widths.pop() if len(widths) == 1 else -1)


def _like_width(like: str) -> int:
    """How many characters a LIKE pattern consumes. Character classes count as one."""
    n, i = 0, 0
    while i < len(like):
        if like[i] == "[":
            close = like.find("]", i + 1)
            # a class whose first member is ']' , or the escaped-metachar forms
            if close == i + 1:
                close = like.find("]", i + 2)
            if close == -1:
                return -1
            i = close + 1
        else:
            i += 1
        n += 1
    return n


def _translate_sequence(body: str) -> tuple[list[str], int]:
    return _combine(_Parser(body).parse())


def translate(regex_pattern: str) -> tuple[list[str], list[str]]:
    """Return (like_patterns, not_like_patterns) equivalent to an anchored regex.

    The predicate the caller builds is::

        (expr LIKE p1 OR expr LIKE p2 ...) AND expr NOT LIKE n1 AND expr NOT LIKE n2 ...

    Raises RegexNotTranslatable, naming the construct, when no exact equivalent exists.
    """
    if not regex_pattern:
        raise RegexNotTranslatable("an empty pattern")
    if not (regex_pattern.startswith("^") and regex_pattern.endswith("$")):
        # Without both anchors the regex is a substring search, and LIKE's leading/trailing
        # wildcards would change which rows match in ways that depend on the pattern.
        raise RegexNotTranslatable("a pattern that is not anchored with both ^ and $")
    body = regex_pattern[1:-1]

    elements = _Parser(body).parse()

    # A negative lookahead becomes NOT LIKE terms: the prefix before it as wildcards, the
    # excluded branch, then the remainder as wildcards. That is exact only when both the
    # prefix and the remainder have a known width.
    not_likes: list[str] = []
    for idx, el in enumerate(elements):
        if not el.lookahead:
            continue
        before, after = elements[:idx], elements[idx + 1 :]
        before_combos, _ = _combine(before)
        after_combos, after_width = _combine(after)
        if after_width < 0:
            # Each prefix combo is already a concrete fragment, so prefixes of differing
            # width are fine -- it is the text AFTER the lookahead that has to line up.
            raise RegexNotTranslatable(
                "a negative lookahead whose following text is not fixed-width"
            )
        if el.width < 0:
            raise RegexNotTranslatable("a negative lookahead whose branches differ in width")
        for excluded in el.fragments:
            if _like_width(excluded) != el.width:
                raise RegexNotTranslatable("a negative lookahead branch of uneven width")
            # The lookahead asserts at a position; what follows it must still be consumed.
            tail = after_width - el.width
            if tail < 0:
                raise RegexNotTranslatable(
                    "a negative lookahead longer than the text that follows it"
                )
            for prefix in before_combos:
                not_likes.append(prefix + excluded + ("_" * tail))
        if len(not_likes) > MAX_ALTERNATIVES:
            raise RegexNotTranslatable(
                f"a pattern expanding to more than {MAX_ALTERNATIVES} LIKE terms"
            )

    likes, _ = _combine(elements)
    if not likes:
        raise RegexNotTranslatable("a pattern that matches nothing")
    return likes, not_likes
