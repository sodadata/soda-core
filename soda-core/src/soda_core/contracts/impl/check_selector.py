from __future__ import annotations

import fnmatch
from typing import Optional

from soda_core.common.exceptions import SodaCoreException


class CheckSelectorParseException(SodaCoreException):
    """Indicates an invalid check selector expression."""


class CheckSelector:
    """Selects checks by matching field values.

    Multiple selectors are grouped by field:
    - Same field: OR (at least one must match)
    - Different fields: AND (all groups must match)
    """

    SUPPORTED_FIELDS = {"type", "name", "column", "path", "qualifier"}
    ATTRIBUTES_PREFIX = "attributes."

    def __init__(self, field: str, value: str, raw: str):
        self.field = field
        self.value = value
        self.raw = raw
        self._selector_list = self._parse_list_value(value)

    def __eq__(self, other):
        if not isinstance(other, CheckSelector):
            return NotImplemented
        return self.field == other.field and self.value == other.value

    def __repr__(self):
        return f"CheckSelector({self.field!r}, {self.value!r})"

    @classmethod
    def parse(cls, expression: str) -> CheckSelector:
        """Parse 'key=value' into a CheckSelector.

        Raises CheckSelectorParseException on invalid syntax.
        """
        if "=" not in expression:
            raise CheckSelectorParseException(f"Invalid check filter '{expression}': expected key=value format")

        field, value = expression.split("=", 1)
        field = field.strip()
        value = value.strip()

        if not field:
            raise CheckSelectorParseException(f"Invalid check filter '{expression}': empty field name")

        if field not in cls.SUPPORTED_FIELDS and not field.startswith(cls.ATTRIBUTES_PREFIX):
            raise CheckSelectorParseException(
                f"Invalid check filter '{expression}': unknown field '{field}'. "
                f"Supported: {', '.join(sorted(cls.SUPPORTED_FIELDS))}, {cls.ATTRIBUTES_PREFIX}<key>"
            )

        return cls(field=field, value=value, raw=expression)

    @classmethod
    def parse_all(cls, expressions: Optional[list[str]]) -> list[CheckSelector]:
        """Parse multiple expressions.

        Raises CheckSelectorParseException on the first invalid expression.
        """
        if not expressions:
            return []
        return [cls.parse(expr) for expr in expressions]

    @classmethod
    def from_check_paths(cls, check_paths: Optional[list[str]]) -> list[CheckSelector]:
        """Convert --check-paths values to path selectors."""
        if not check_paths:
            return []
        return [cls(field="path", value=path, raw=f"path={path}") for path in check_paths]

    def matches(self, check_impl) -> bool:
        """Returns True if the given CheckImpl matches this selector."""
        check_value = self._get_check_value(check_impl)
        if check_value is None:
            return False
        if isinstance(check_value, list):
            selector_list = self._selector_list
            if selector_list is not None:
                # Full list match: exact set equality, no wildcards
                return set(check_value) == set(selector_list)
            else:
                # Member match: any element matches (with wildcards)
                return any(self._values_match(item, self.value) for item in check_value)
        return self._values_match(check_value, self.value)

    def _get_check_value(self, check_impl) -> Optional[str | list[str]]:
        """Extract the relevant field value from a CheckImpl."""
        if self.field == "type":
            return check_impl.type
        elif self.field == "name":
            return check_impl.name
        elif self.field == "column":
            return check_impl.column_impl.column_yaml.name if check_impl.column_impl else None
        elif self.field == "path":
            return check_impl.path
        elif self.field == "qualifier":
            return check_impl.check_yaml.qualifier
        elif self.field.startswith(self.ATTRIBUTES_PREFIX):
            attr_key = self.field[len(self.ATTRIBUTES_PREFIX) :]
            attr_value = check_impl.attributes.get(attr_key)
            if attr_value is None:
                return None
            if isinstance(attr_value, list):
                return [str(item) for item in attr_value]
            return str(attr_value)
        return None

    @staticmethod
    def _parse_list_value(value: str) -> Optional[list[str]]:
        """Parse '[a,b,c]' into ['a','b','c']. Returns None if not list syntax.

        Supports quoted elements for values containing commas or spaces:
            ["a,b", c, "d e"] -> ['a,b', 'c', 'd e']
        """
        if not (value.startswith("[") and value.endswith("]")):
            return None
        inner = value[1:-1]
        if not inner.strip():
            return []
        items = []
        current = []
        in_quotes = False
        for char in inner:
            if char == '"':
                in_quotes = not in_quotes
            elif char == "," and not in_quotes:
                items.append("".join(current).strip())
                current = []
            else:
                current.append(char)
        if in_quotes:
            raise CheckSelectorParseException(f"Invalid list syntax in selector value {value!r}: unterminated quote")
        items.append("".join(current).strip())
        if any(item == "" for item in items):
            raise CheckSelectorParseException(
                f"Invalid list syntax in selector value {value!r}: empty elements are not allowed"
            )
        return items

    def _values_match(self, check_value: str, selector_value: str) -> bool:
        """Compare values. Uses fnmatch if selector_value contains wildcards."""
        # Escape '[' so fnmatch treats it literally — only * and ? are wildcards
        escaped = selector_value.replace("[", "[[]")
        return fnmatch.fnmatchcase(check_value, escaped)

    @staticmethod
    def all_match(selectors: list[CheckSelector], check_impl) -> bool:
        """Check if all selector groups match (AND across fields, OR within field)."""
        if not selectors:
            return True

        # Group by field
        groups: dict[str, list[CheckSelector]] = {}
        for s in selectors:
            groups.setdefault(s.field, []).append(s)

        # AND across groups, OR within each group
        for field, group in groups.items():
            if not any(s.matches(check_impl) for s in group):
                return False
        return True
