"""Scopes: named, filtered views of a check collection's dataset.

A contract declares scopes under its top-level ``scopes`` key and a check picks one with
``scope: <key>``. Every collection also has a base scope, its unscoped view, built from
the top-level ``filter`` and ``check_attributes``. A scope is active once it has a CTE to
select from. Core activates only the base scope; a declared scope stays inactive unless
an extension activates it, and checks in an inactive scope are skipped. Only contracts
support scopes: in any other kind, ``scopes`` and a check's ``scope`` are parse errors.

This module imports only from ``soda_core.common`` at runtime, so ``base.py`` can import
it at module level.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from numbers import Number
from typing import TYPE_CHECKING, Any, Mapping, Optional

from ruamel.yaml.comments import TaggedScalar
from soda_core.common.filtered_cte import filtered_cte_alias
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.logs import Location
from soda_core.common.yaml import VariableResolver, YamlObject

if TYPE_CHECKING:
    from soda_core.common.sql_ast import CTE
    from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl

logger: logging.Logger = soda_logger

BASE_SCOPE_KEY: str = "base"
RESERVED_SCOPE_KEYS: frozenset[str] = frozenset({"base", "true", "false", "null", "yes", "no", "on", "off", "y", "n"})
SCOPE_KEY_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
# The key of the placeholder scope for a check whose ``scope`` is not a valid scope key. The file logged an error.
INVALID_SCOPE_KEY: str = "<invalid>"

# The keys a scope accepts, and the keys its schedule accepts.
SCOPE_YAML_KEYS: tuple[str, ...] = ("name", "description", "filter", "schedule", "check_attributes")
SCHEDULE_YAML_KEYS: tuple[str, ...] = ("cron", "timezone", "variables")


class ScheduleYaml:
    """A scope's ``schedule`` mapping. The readers log a value of the wrong type at its key and read it as None."""

    def __init__(self, schedule_yaml_object: YamlObject, scope_text: str, written: dict) -> None:
        self.schedule_yaml_object: YamlObject = schedule_yaml_object
        self.location: Optional[Location] = schedule_yaml_object.location
        _log_unknown_keys(schedule_yaml_object, written, SCHEDULE_YAML_KEYS, f"the schedule of {scope_text}")
        _log_null_values(schedule_yaml_object, written, SCHEDULE_YAML_KEYS)
        if "cron" not in written:
            log_scope_error(f"The schedule of {scope_text} has no 'cron'", self.location)
        self.cron: Optional[str] = schedule_yaml_object.read_string_opt("cron")
        self.timezone: Optional[str] = schedule_yaml_object.read_string_opt("timezone")
        variables: Optional[YamlObject] = schedule_yaml_object.read_object_opt("variables")
        self.variables: Optional[dict[str, Any]] = None
        if isinstance(variables, YamlObject):
            _log_invalid_schedule_variables(variables, written["variables"], scope_text)
            self.variables = variables.to_dict()


class ScopeYaml:
    """One entry of ``scopes``. Parsing logs an error for each scope rule the entry breaks, and the readers log a
    value of the wrong type at its key and read it as None, or as ``{}`` for ``check_attributes``.

    ``key`` is the mapping key exactly as the YAML parser returned it, so it may be a bool, None or an int.
    """

    def __init__(
        self,
        key: Any,
        scope_yaml_object: Optional[YamlObject],
        location: Optional[Location],
        written: Optional[dict] = None,
    ) -> None:
        self.key: Any = key
        self.location: Optional[Location] = location
        self.scope_yaml_object: Optional[YamlObject] = scope_yaml_object
        self.name: Optional[str] = None
        self.description: Optional[str] = None
        self.filter: Optional[str] = None
        self.check_attributes: dict[str, Any] = {}
        self.schedule: Optional[ScheduleYaml] = None
        if scope_yaml_object is None:
            return

        # Null, required and unknown keys are checked on the mapping as written, before variables resolve, so a
        # variable that resolves to nothing logs only the resolver's error.
        written = written if written is not None else scope_yaml_object.yaml_dict
        scope_text: str = f"scope {_value_text(key)}"
        _log_unknown_keys(scope_yaml_object, written, SCOPE_YAML_KEYS, scope_text)
        _log_null_values(scope_yaml_object, written, SCOPE_YAML_KEYS)
        if "name" not in written:
            log_scope_error(f"Scope {_value_text(key)} has no 'name'", location)
        self.name = scope_yaml_object.read_string_opt("name")
        self.description = scope_yaml_object.read_string_opt("description")
        filter = scope_yaml_object.read_string_opt("filter")
        if isinstance(filter, str) and filter.strip():
            self.filter = filter.strip()
        check_attributes = scope_yaml_object.read_object_opt("check_attributes")
        self.check_attributes = check_attributes.to_dict() if isinstance(check_attributes, YamlObject) else {}
        schedule = scope_yaml_object.read_object_opt("schedule")
        self.schedule = (
            ScheduleYaml(schedule, scope_text, written["schedule"]) if isinstance(schedule, YamlObject) else None
        )

    @classmethod
    def parse_scopes(cls, contract_yaml_object: YamlObject) -> dict[Any, ScopeYaml]:
        """One ``ScopeYaml`` per ``scopes`` key in file order, ``{}`` when ``scopes`` is absent or not a mapping.
        Logs an error for each rule a key or a scope breaks, at the location of what breaks it."""
        _log_null_values(contract_yaml_object, contract_yaml_object.yaml_dict, ("scopes",))
        scopes_yaml_object = contract_yaml_object.read_object_opt("scopes")
        if not isinstance(scopes_yaml_object, YamlObject):
            return {}
        scope_yamls: dict[Any, ScopeYaml] = {}
        for key, scope_yaml_object in scopes_yaml_object.items():
            # Each body as written: reading the scopes resolves a body that is a lone variable, and wrapping a body
            # into scope_yaml_object resolves its values.
            written: Any = contract_yaml_object.yaml_dict["scopes"][key]
            location = scopes_yaml_object.create_location_from_yaml_dict_key(key)
            key_error = scope_key_error(key)
            if key_error:
                log_scope_error(f"Invalid scope key {_value_text(key)}: {key_error}", location)
            if not isinstance(written, dict):
                log_scope_error(
                    f"Scope {_value_text(key)} must be an object with a 'name', but was {_type_text(written)}", location
                )
            scope_yamls[key] = cls(
                key=key,
                scope_yaml_object=scope_yaml_object if isinstance(scope_yaml_object, YamlObject) else None,
                location=location,
                written=written if isinstance(written, dict) else None,
            )
        return scope_yamls


def _log_null_values(yaml_object: YamlObject, written: dict, keys: tuple[str, ...]) -> None:
    """Omitting a key already means it is not set, so an explicit null is rejected rather than read as absent."""
    for key in keys:
        if key in written and written[key] is None:
            log_scope_error(f"YAML key '{key}' must not be null", yaml_object.create_location_from_yaml_dict_key(key))


def _log_unknown_keys(yaml_object: YamlObject, written: dict, allowed_keys: tuple[str, ...], where: str) -> None:
    for key in written:
        if key not in allowed_keys:
            log_scope_error(
                f"Unknown key {_value_text(key)} in {where}", yaml_object.create_location_from_yaml_dict_key(key)
            )


def _log_invalid_schedule_variables(variables: YamlObject, written: dict, scope_text: str) -> None:
    """A schedule's variables map string names to strings or numbers, as the variables of a run do. Checked as
    written, before variables resolve."""
    for name, value in written.items():
        location = variables.create_location_from_yaml_dict_key(name)
        if not isinstance(name, str):
            log_scope_error(
                f"Variable name {_value_text(name)} in the schedule of {scope_text} must be a string, "
                f"but YAML reads it as {_type_text(name)}",
                location,
            )
        # JSON numbers exclude booleans, which Python counts as numbers.
        if isinstance(value, bool) or not isinstance(value, (str, Number)):
            log_scope_error(
                f"Variable {_value_text(name)} in the schedule of {scope_text} must be a string or a number, "
                f"but was {_type_text(value)}",
                location,
            )


def scope_key_error(key: Any) -> Optional[str]:
    """Why ``key`` cannot be a scope key, or None when it can.

    ruamel reads ``true``, ``null`` and ``0`` as a bool, None and an int, so any key that is not a ``str`` fails
    before the reserved words and the pattern.
    """
    if not isinstance(key, str):
        return f"a scope key must be a string, but YAML reads this one as {_type_text(key)}"
    if key == BASE_SCOPE_KEY:
        return "'base' is reserved for the checks without a scope"
    if key in RESERVED_SCOPE_KEYS:
        return f"'{key}' is reserved, because YAML parsers can read it as a boolean or null"
    if re.search(VariableResolver.VARIABLE_PATTERN, key):
        return "a scope key is fixed, so it cannot use a variable"
    if not SCOPE_KEY_PATTERN.fullmatch(key):
        return (
            "a scope key starts with a lowercase letter, followed by at most 63 lowercase letters, digits, '_' or '-'"
        )
    return None


def check_scope_error(scope: Any, scopes: Mapping) -> Optional[str]:
    """Why a check's ``scope`` value, when it is not None, names no scope the check can run in. None when it names
    one of ``scopes``, the declared scopes by key.
    """
    if not isinstance(scope, str):
        # A mapping or a list can print long, so only a scalar shows its value.
        value_text = "" if isinstance(scope, (dict, list)) else f": {_value_text(scope)}"
        return f"Check 'scope' must name a declared scope, but was {_type_text(scope)}{value_text}"
    if scope in RESERVED_SCOPE_KEYS:
        return f"Invalid check scope {_value_text(scope)}: {scope_key_error(scope)}"
    if re.search(VariableResolver.VARIABLE_PATTERN, scope):
        return f"Check 'scope' cannot use a variable, but was {_value_text(scope)}. Name a declared scope key"
    declared_keys = [key for key in scopes if isinstance(key, str) and key != BASE_SCOPE_KEY]
    if scope not in declared_keys:
        # The keys that were rejected already logged an error of their own, so the list leaves them out.
        valid_keys = [key for key in declared_keys if scope_key_error(key) is None]
        if valid_keys:
            declared = f"Declared scopes: {_value_text(valid_keys)}"
        elif declared_keys:
            declared = "No valid scopes are declared"
        else:
            declared = "No scopes are declared"
        return f"Check references unknown scope {_value_text(scope)}. {declared}"
    return None


def check_scope_location(check_yaml_object: Optional[YamlObject]) -> Optional[Location]:
    """Where a check body sets ``scope``."""
    if not isinstance(check_yaml_object, YamlObject):
        return None
    return check_yaml_object.create_location_from_yaml_dict_key("scope")


def unsupported_scopes_error(kind: Optional[str]) -> str:
    """The error for scope input in a file whose kind does not support scopes."""
    return f"Scopes are only supported in contracts, not in kind '{kind}'"


def log_scope_error(message: str, location: Optional[Location]) -> None:
    logger.error(msg=message, extra={ExtraKeys.LOCATION: location})


def _sets_scope(check_yaml_object: Any) -> bool:
    return (
        isinstance(check_yaml_object, YamlObject)
        and isinstance(check_yaml_object.yaml_dict, dict)
        and ("scope" in check_yaml_object.yaml_dict)
    )


class ScopeHandling:
    """How a kind of check collection handles scopes. A kind picks one on its impl class as ``scope_support``:
    ``NoScopeSupport`` by default, ``ScopeSupport`` for contracts. The YAML of the kind and its impl call it, so no
    code outside this module asks whether a kind supports scopes."""

    def prepare(self, collection_yaml_object: YamlObject, kind: Optional[str]) -> None:
        """Runs once before the file is parsed."""

    def parse_scopes(self, collection_yaml_object: YamlObject) -> dict[Any, ScopeYaml]:
        """The declared scopes by key, with an error logged for each rule they break."""
        return {}

    def check_scope_input_error(self, check_yaml_object: Any, scopes: Mapping, kind: Optional[str]) -> Optional[str]:
        """Why the ``scope`` a check body sets is invalid, or None when it is valid or the check sets none."""
        return None

    def validate_check_scope(self, check_yaml: Any, scopes: Mapping, kind: Optional[str]) -> None:
        """Logs an error for an invalid check ``scope``, once per check."""
        error: Optional[str] = self.check_scope_input_error(check_yaml.check_yaml_object, scopes, kind)
        if error:
            log_scope_error(error, check_scope_location(check_yaml.check_yaml_object))
        check_yaml.scope_validated = True

    def scope_for(self, check_yaml: Any, base_scope: Scope, scopes: Mapping, kind: Optional[str]) -> Scope:
        """The scope a check runs in, never None. A check whose scope the YAML did not validate, one an extension
        parsed itself, is validated here."""
        if not check_yaml.scope_validated:
            self.validate_check_scope(check_yaml, scopes, kind)
        return self._resolve(check_yaml.scope, base_scope, scopes)

    def _resolve(self, scope: Any, base_scope: Scope, scopes: Mapping) -> Scope:
        return base_scope


class NoScopeSupport(ScopeHandling):
    """The default: ``scopes`` and a check's ``scope`` are parse errors, and every check runs in the base scope."""

    def prepare(self, collection_yaml_object: YamlObject, kind: Optional[str]) -> None:
        if isinstance(collection_yaml_object, YamlObject) and "scopes" in collection_yaml_object.yaml_dict:
            log_scope_error(
                unsupported_scopes_error(kind), collection_yaml_object.create_location_from_yaml_dict_key("scopes")
            )

    def check_scope_input_error(self, check_yaml_object: Any, scopes: Mapping, kind: Optional[str]) -> Optional[str]:
        return unsupported_scopes_error(kind) if _sets_scope(check_yaml_object) else None


class ScopeSupport(ScopeHandling):
    """Contracts: declared scopes, and a check that names one runs in it."""

    def prepare(self, collection_yaml_object: YamlObject, kind: Optional[str]) -> None:
        # A check's scope names a fixed key, so it is read as written: a variable in it stays text and is rejected.
        if isinstance(collection_yaml_object, YamlObject):
            collection_yaml_object.yaml_source.unresolved_keys.add("scope")

    def parse_scopes(self, collection_yaml_object: YamlObject) -> dict[Any, ScopeYaml]:
        return ScopeYaml.parse_scopes(collection_yaml_object)

    def check_scope_input_error(self, check_yaml_object: Any, scopes: Mapping, kind: Optional[str]) -> Optional[str]:
        if not _sets_scope(check_yaml_object):
            return None
        scope: Any = check_yaml_object.yaml_dict["scope"]
        if scope is None:
            return "Check 'scope' must name a declared scope, but was null"
        return check_scope_error(scope, scopes)

    def _resolve(self, scope: Any, base_scope: Scope, scopes: Mapping) -> Scope:
        # A value that names no declared scope logged an error and gets an inactive placeholder, so the check is
        # skipped.
        if scope is None or scope == BASE_SCOPE_KEY:
            return base_scope
        if isinstance(scope, str) and scope in scopes:
            return scopes[scope]
        return Scope(key=scope if isinstance(scope, str) and SCOPE_KEY_PATTERN.fullmatch(scope) else INVALID_SCOPE_KEY)


def _type_text(value: Any) -> str:
    """What YAML made of ``value``, for error messages."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, Number):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, dict):
        return "an object"
    if isinstance(value, list):
        return "a list"
    if isinstance(value, TaggedScalar):
        return "a tagged value"
    return f"a {type(value).__name__}"


def _value_text(value: Any) -> str:
    """``value`` for an error message: a string quoted, a scalar as YAML writes it, and never a lone surrogate,
    which a log handler cannot encode."""
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif value is None:
        text = "null"
    elif isinstance(value, (str, list)):
        text = repr(value)
    else:
        try:
            text = str(value)
        except ValueError:
            # An int past Python's digit limit.
            text = f"<{type(value).__name__}>"
    return text.encode("utf-8", "backslashreplace").decode("utf-8")


@dataclass(eq=False)
class Scope:
    """A scope of one check collection. Equality and hashing are object identity."""

    key: str
    name: Optional[str] = None
    description: Optional[str] = None
    filter: Optional[str] = None
    check_attributes: dict[str, Any] = field(default_factory=dict)
    schedule: Optional[ScheduleYaml] = None
    cte: Optional[CTE] = None
    row_count_metric: Optional[RowCountMetricImpl] = None

    @classmethod
    def from_yaml(cls, scope_yaml: ScopeYaml) -> Scope:
        if not isinstance(scope_yaml.key, str):
            raise TypeError(f"Scope key must be a string, but was {type(scope_yaml.key).__name__}")
        return cls(
            key=scope_yaml.key,
            name=scope_yaml.name,
            description=scope_yaml.description,
            filter=scope_yaml.filter,
            check_attributes=scope_yaml.check_attributes,
            schedule=scope_yaml.schedule,
        )

    @property
    def is_base(self) -> bool:
        return self.key == BASE_SCOPE_KEY

    @property
    def is_active(self) -> bool:
        return self.cte is not None

    def activate(self, cte: CTE, row_count_metric: Optional[RowCountMetricImpl]) -> None:
        self.cte = cte
        self.row_count_metric = row_count_metric

    def cte_alias(self) -> str:
        return filtered_cte_alias(None if self.is_base else self.key)
