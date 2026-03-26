from __future__ import annotations

import fnmatch
import logging
from typing import Optional

logger = logging.getLogger("soda")


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

    def __eq__(self, other):
        if not isinstance(other, CheckSelector):
            return NotImplemented
        return self.field == other.field and self.value == other.value

    def __repr__(self):
        return f"CheckSelector({self.field!r}, {self.value!r})"

    @classmethod
    def parse(cls, expression: str) -> Optional[CheckSelector]:
        """Parse 'key=value' into a CheckSelector. Returns None on error."""
        if "=" not in expression:
            logger.error(f"Invalid check filter '{expression}': expected key=value format")
            return None

        field, value = expression.split("=", 1)
        field = field.strip()
        value = value.strip()

        if not field:
            logger.error(f"Invalid check filter '{expression}': empty field name")
            return None

        if field not in cls.SUPPORTED_FIELDS and not field.startswith(cls.ATTRIBUTES_PREFIX):
            logger.error(
                f"Invalid check filter '{expression}': unknown field '{field}'. "
                f"Supported: {', '.join(sorted(cls.SUPPORTED_FIELDS))}, attributes.<key>"
            )
            return None

        return cls(field=field, value=value, raw=expression)

    @classmethod
    def parse_all(cls, expressions: Optional[list[str]]) -> Optional[list[CheckSelector]]:
        """Parse multiple expressions. Returns None if any is invalid."""
        if not expressions:
            return []
        selectors = []
        has_errors = False
        for expr in expressions:
            selector = cls.parse(expr)
            if selector is None:
                has_errors = True
            else:
                selectors.append(selector)
        return None if has_errors else selectors

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
        return self._values_match(check_value, self.value)

    def _get_check_value(self, check_impl) -> Optional[str]:
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
            return str(attr_value) if attr_value is not None else None
        return None

    def _values_match(self, check_value: str, selector_value: str) -> bool:
        """Compare values. Uses fnmatch if selector_value contains wildcards."""
        if "*" in selector_value or "?" in selector_value:
            return fnmatch.fnmatchcase(check_value, selector_value)
        return check_value == selector_value

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
