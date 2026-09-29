"""Scopes: named, filtered views of a check collection's dataset.

A contract declares scopes under its top-level ``scopes`` key and a check picks one with
``scope: <key>``. Every collection also has a base scope, its unscoped view, built from
the top-level ``filter`` and ``check_attributes``. A scope is active once it has a CTE to
select from. Core activates only the base scope; a declared scope stays inactive unless
an extension activates it, and checks in an inactive scope are skipped.

This module imports only from ``soda_core.common`` at runtime, so ``base.py`` can import
it at module level.
"""

from __future__ import annotations

import copy
import logging
import re
from dataclasses import dataclass, field
from numbers import Number
from typing import TYPE_CHECKING, Any, Mapping, Optional

from ruamel.yaml.comments import TaggedScalar
from soda_core.common.filtered_cte import filtered_cte_alias
from soda_core.common.logging_constants import ExtraKeys, soda_logger
from soda_core.common.logs import Location
from soda_core.common.yaml import VariableResolver, YamlList, YamlObject, YamlSource

if TYPE_CHECKING:
    from soda_core.common.sql_ast import CTE
    from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl

logger: logging.Logger = soda_logger

BASE_SCOPE_KEY: str = "base"
RESERVED_SCOPE_KEYS: frozenset[str] = frozenset({"base", "true", "false", "null", "yes", "no", "on", "off", "y", "n"})
SCOPE_KEY_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

# The keys a scope accepts, and the keys its schedule accepts.
SCOPE_YAML_KEYS: tuple[str, ...] = ("name", "description", "filter", "schedule", "check_attributes")
SCHEDULE_YAML_KEYS: tuple[str, ...] = ("cron", "timezone", "variables")


def mark_scope_support(yaml_source: YamlSource, supports_scopes: bool) -> None:
    """Records on ``yaml_source`` whether the kind of its file supports scopes.

    Reads of scope input from that file resolve variables only when it does, so a kind without support never logs
    or fails on its scope input. ``ContractYaml`` records it before it reads any. A file nobody recorded it for
    reads its scope input as written.
    """
    yaml_source._supports_scopes = supports_scopes


def scopes_supported(yaml_source: YamlSource) -> bool:
    """What ``mark_scope_support`` recorded on ``yaml_source``, False when nothing did."""
    return getattr(yaml_source, "_supports_scopes", False)


def _wrap_scope_value(yaml_object: YamlObject, value: Any, location: Optional[Location]) -> Any:
    """``yaml_object._yaml_wrap(value, location)`` in a file whose kind supports scopes, which resolves variables
    one level deep. In any other file the same copy and wrapper without resolving them.

    Both copy a mapping or a list, as every read of one does on origin. A chain of mappings that each merge and
    reference the one before copies in exponential time, in scope input as in a check body on origin. Nothing here
    bounds it.
    """
    if getattr(yaml_object.yaml_source, "_supports_scopes", False):
        return yaml_object._yaml_wrap(value, location=location)
    if isinstance(value, dict):
        return YamlObject(yaml_source=yaml_object.yaml_source, yaml_dict=copy.deepcopy(value))
    if isinstance(value, list):
        return YamlList(yaml_source=yaml_object.yaml_source, yaml_list=copy.deepcopy(value))
    return value


def _key_location(yaml_object: YamlObject, key: Any) -> Optional[Location]:
    """Where ``key`` sits in ``yaml_object``, or where the mapping starts when ruamel kept no position for it.

    ruamel keeps none for a key merged in with ``<<``, none at all for a mapping of merged keys only, and none
    for a tagged key once the mapping is copied. ``create_location_from_yaml_dict_key`` then raises.
    """
    try:
        return yaml_object.create_location_from_yaml_dict_key(key)
    except (KeyError, TypeError):
        return yaml_object.location


def _read_scope_value(yaml_object: YamlObject, key: Any) -> Any:
    """``yaml_object.read_value(key)`` for a key that scopes add, located with ``_key_location`` and wrapped
    with ``_wrap_scope_value``.

    Origin never reads these keys, so a file that runs on origin must not fail on them. Every other read keeps
    raising as on origin for a key ruamel kept no position for.
    """
    if key not in yaml_object.yaml_dict:
        return None
    return _wrap_scope_value(yaml_object, yaml_object.yaml_dict.get(key), _key_location(yaml_object, key))


def _read_scope_field(yaml_object: YamlObject, key: str) -> Any:
    """``_read_scope_value`` for a key of a scope body or a schedule.

    A value nested deeper than the copy on read can recurse reads as None, like a value of the wrong type.
    """
    try:
        return _read_scope_value(yaml_object, key)
    except RecursionError:
        return None


def read_check_scope(check_yaml_object: YamlObject) -> Any:
    """A check's ``scope``, with each mapping and list in it copied into a plain dict and list, so the value
    prints the same on every run.

    In a file whose kind supports scopes, a string is variable-resolved on read like any other check key; in any
    other file it stays as origin's read of the check body left it. Strings inside a mapping or a list stay as
    written: origin resolves one level into a check and never reads ``scope``, and such a value names no scope.
    The copy shares what the YAML shares through anchors, so a small file never expands into a large value.
    A value nested deeper than the copy can recurse stays as parsed. It is never None, which would run the
    check unscoped, and never a string, so it names no declared scope.
    """
    value = check_yaml_object.yaml_dict.get("scope")
    if isinstance(value, (dict, list)):
        try:
            return _plain_scope_value(value, {})
        except RecursionError:
            return value
    return _read_scope_value(check_yaml_object, "scope")


def _plain_scope_value(value: Any, copies: dict[int, Any]) -> Any:
    """``value`` with each mapping and list copied once into a plain dict or list, so anchors stay shared."""
    if not isinstance(value, (dict, list)):
        return value
    if id(value) in copies:
        return copies[id(value)]
    if isinstance(value, dict):
        plain_dict: dict = {}
        copies[id(value)] = plain_dict
        for key, element in value.items():
            plain_dict[key] = _plain_scope_value(element, copies)
        return plain_dict
    plain_list: list = []
    copies[id(value)] = plain_list
    for element in value:
        plain_list.append(_plain_scope_value(element, copies))
    return plain_list


# The most characters str() may print for a mapping or list scope value; see scope_value_text.
_SCOPE_VALUE_TEXT_BUDGET: int = 4096


def scope_value_text(value: Any) -> str:
    """``str()`` of a check's ``scope`` value, which ``scope_for`` compares with the base key and gives to a
    placeholder scope.

    ``str()`` prints a value shared through anchors once per reference, so a mapping or list that would print
    past the budget reads as its type name. So does a value ``str()`` rejects: an int past Python's digit limit,
    also inside a mapping or a list, or a value nested too deep to print.
    """
    try:
        if isinstance(value, (dict, list)) and not _prints_within(value, _SCOPE_VALUE_TEXT_BUDGET):
            return f"<{type(value).__name__}>"
        return str(value)
    except (ValueError, RecursionError):
        return f"<{type(value).__name__}>"


def _prints_within(value: Any, budget: int) -> bool:
    """Whether ``str(value)`` stays within about ``budget`` characters, counted without building it.

    It visits the value reference by reference, as ``str()`` does, and stops once the count passes the budget,
    so a value that holds itself ends too.
    """
    pending: list = [value]
    while pending:
        node = pending.pop()
        if isinstance(node, dict):
            budget -= 2 + 4 * len(node)
            if budget >= 0:
                pending.extend(node.keys())
                pending.extend(node.values())
        elif isinstance(node, (list, tuple)):
            budget -= 2 + 2 * len(node)
            if budget >= 0:
                pending.extend(node)
        else:
            budget -= len(repr(node))
        if budget < 0:
            return False
    return True


class ScheduleYaml:
    """A ``schedule`` mapping, read without validation."""

    def __init__(self, schedule_yaml_object: YamlObject) -> None:
        self.schedule_yaml_object: YamlObject = schedule_yaml_object
        self.location: Optional[Location] = schedule_yaml_object.location
        cron = _read_scope_field(schedule_yaml_object, "cron")
        self.cron: Optional[str] = cron if isinstance(cron, str) else None
        timezone = _read_scope_field(schedule_yaml_object, "timezone")
        self.timezone: Optional[str] = timezone if isinstance(timezone, str) else None
        variables = _read_scope_field(schedule_yaml_object, "variables")
        self.variables: Optional[dict[str, Any]] = variables.to_dict() if isinstance(variables, YamlObject) else None


class ScopeYaml:
    """One entry of ``scopes``, read without validation.

    ``key`` is the mapping key exactly as the YAML parser returned it, so it may be a bool, None
    or an int. Values of the wrong type read as None, or as ``{}`` for ``check_attributes``.
    """

    def __init__(self, key: Any, scope_yaml_object: Optional[YamlObject], location: Optional[Location]) -> None:
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

        name = _read_scope_field(scope_yaml_object, "name")
        self.name = name if isinstance(name, str) else None
        description = _read_scope_field(scope_yaml_object, "description")
        self.description = description if isinstance(description, str) else None
        filter = _read_scope_field(scope_yaml_object, "filter")
        if isinstance(filter, str) and filter.strip():
            self.filter = filter.strip()
        check_attributes = _read_scope_field(scope_yaml_object, "check_attributes")
        self.check_attributes = check_attributes.to_dict() if isinstance(check_attributes, YamlObject) else {}
        schedule = _read_scope_field(scope_yaml_object, "schedule")
        self.schedule = ScheduleYaml(schedule) if isinstance(schedule, YamlObject) else None

    @classmethod
    def parse_scopes(cls, contract_yaml_object: YamlObject) -> dict[Any, ScopeYaml]:
        """One ``ScopeYaml`` per ``scopes`` key in file order, ``{}`` when ``scopes`` is absent, not
        a mapping, or nested deeper than the copy on read can recurse."""
        try:
            scopes_yaml_object = _read_scope_value(contract_yaml_object, "scopes")
            if not isinstance(scopes_yaml_object, YamlObject):
                return {}
            # What YamlObject.items() returns, wrapped with _wrap_scope_value.
            scope_bodies = [
                (key, _wrap_scope_value(scopes_yaml_object, body, location=None))
                for key, body in scopes_yaml_object.yaml_dict.items()
            ]
        except RecursionError:
            return {}
        scope_yamls: dict[Any, ScopeYaml] = {}
        for key, scope_yaml_object in scope_bodies:
            scope_yamls[key] = cls(
                key=key,
                scope_yaml_object=scope_yaml_object if isinstance(scope_yaml_object, YamlObject) else None,
                location=_key_location(scopes_yaml_object, key),
            )
        return scope_yamls


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
    if not SCOPE_KEY_PATTERN.fullmatch(key):
        return (
            "a scope key starts with a lowercase letter, followed by at most 63 lowercase letters, digits, '_' or '-'"
        )
    return None


def validate_scopes(contract_yaml_object: YamlObject) -> None:
    """Logs an error for each rule the ``scopes`` of a contract breaks, at the location of what breaks it.

    Reads the values as written, before variables are resolved, so the same value is checked in every run. Only a
    kind that supports scopes calls this.
    """
    yaml_dict = contract_yaml_object.yaml_dict
    if "scopes" not in yaml_dict:
        return
    scopes = yaml_dict.get("scopes")
    if not isinstance(scopes, dict):
        log_scope_error(
            f"'scopes' must be an object that maps scope keys to scopes, but was {_type_text(scopes)}",
            _key_location(contract_yaml_object, "scopes"),
        )
        return
    scopes_yaml_object = YamlObject(yaml_source=contract_yaml_object.yaml_source, yaml_dict=scopes)
    for key, body in scopes.items():
        location = _key_location(scopes_yaml_object, key)
        key_error = scope_key_error(key)
        if key_error:
            log_scope_error(f"Invalid scope key {_value_text(key)}: {key_error}", location)
        _validate_scope_body(scopes_yaml_object.yaml_source, _value_text(key), body, location)


def _validate_scope_body(yaml_source: YamlSource, key_text: str, body: Any, location: Optional[Location]) -> None:
    scope = f"scope {key_text}"
    if not isinstance(body, dict):
        log_scope_error(f"Scope {key_text} must be an object with a 'name', but was {_type_text(body)}", location)
        return
    body_yaml_object = YamlObject(yaml_source=yaml_source, yaml_dict=body)
    for key in body:
        if key not in SCOPE_YAML_KEYS:
            log_scope_error(f"Unknown key {_value_text(key)} in {scope}", _key_location(body_yaml_object, key))
    if "name" not in body:
        log_scope_error(f"Scope {key_text} has no 'name'", location)
    for key in ("name", "description", "filter"):
        if key in body and not isinstance(body[key], str):
            log_scope_error(
                f"'{key}' of {scope} must be a string, but was {_type_text(body[key])}",
                _key_location(body_yaml_object, key),
            )
    if "check_attributes" in body and not isinstance(body["check_attributes"], dict):
        log_scope_error(
            f"'check_attributes' of {scope} must be an object, but was {_type_text(body['check_attributes'])}",
            _key_location(body_yaml_object, "check_attributes"),
        )
    if "schedule" in body:
        _validate_schedule(yaml_source, scope, body["schedule"], _key_location(body_yaml_object, "schedule"))


def _validate_schedule(yaml_source: YamlSource, scope: str, schedule: Any, location: Optional[Location]) -> None:
    if not isinstance(schedule, dict):
        log_scope_error(
            f"'schedule' of {scope} must be an object with a 'cron', but was {_type_text(schedule)}", location
        )
        return
    schedule_yaml_object = YamlObject(yaml_source=yaml_source, yaml_dict=schedule)
    for key in schedule:
        if key not in SCHEDULE_YAML_KEYS:
            log_scope_error(
                f"Unknown key {_value_text(key)} in the schedule of {scope}", _key_location(schedule_yaml_object, key)
            )
    if "cron" not in schedule:
        log_scope_error(f"The schedule of {scope} has no 'cron'", location)
    for key in ("cron", "timezone"):
        if key in schedule and not isinstance(schedule[key], str):
            log_scope_error(
                f"'{key}' in the schedule of {scope} must be a string, but was {_type_text(schedule[key])}",
                _key_location(schedule_yaml_object, key),
            )
    if "variables" not in schedule:
        return
    variables = schedule["variables"]
    variables_location = _key_location(schedule_yaml_object, "variables")
    if not isinstance(variables, dict):
        log_scope_error(
            f"'variables' in the schedule of {scope} must be an object, but was {_type_text(variables)}",
            variables_location,
        )
        return
    variables_yaml_object = YamlObject(yaml_source=yaml_source, yaml_dict=variables)
    for name, value in variables.items():
        if not isinstance(name, str):
            log_scope_error(
                f"Variable name {_value_text(name)} in the schedule of {scope} must be a string, "
                f"but YAML reads it as {_type_text(name)}",
                _key_location(variables_yaml_object, name),
            )
        # JSON numbers exclude booleans, which Python counts as numbers.
        if isinstance(value, bool) or not isinstance(value, (str, Number)):
            log_scope_error(
                f"Variable {_value_text(name)} in the schedule of {scope} must be a string or a number, "
                f"but was {_type_text(value)}",
                _key_location(variables_yaml_object, name),
            )


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
    declared_keys = [key for key in scopes if isinstance(key, str) and key != BASE_SCOPE_KEY]
    if scope not in declared_keys:
        declared = f"Declared scopes: {_value_text(declared_keys)}" if declared_keys else "No scopes are declared"
        return f"Check references unknown scope {_value_text(scope)}. {declared}"
    return None


def null_check_scope_error(check_body: Any, yaml_source: YamlSource) -> Optional[str]:
    """Why a check whose ``scope`` reads as null must not run in the base scope. ``check_body`` is the body of the
    check as written.

    None when the body sets no ``scope``, or when it sets a lone reference whose resolving to null logged why.
    """
    if not isinstance(check_body, dict) or "scope" not in check_body:
        return None
    written: Any = check_body["scope"]
    if written is None:
        return "Check 'scope' must name a declared scope, but was null"
    if VariableResolver.logs_unresolved_reference(
        written,
        variable_values=yaml_source.resolve_on_read_variable_values,
        soda_variable_values=yaml_source.resolve_on_read_soda_variable_values,
        use_env_vars=yaml_source.resolve_on_read_use_env_vars,
    ):
        return None
    return f"Check 'scope' must name a declared scope, but {_value_text(written)} resolved to null"


def check_scope_location(check_yaml_object: Optional[YamlObject]) -> Optional[Location]:
    """Where a check body sets ``scope``."""
    if not isinstance(check_yaml_object, YamlObject):
        return None
    return _key_location(check_yaml_object, "scope")


def log_scope_error(message: str, location: Optional[Location]) -> None:
    logger.error(msg=message, extra={ExtraKeys.LOCATION: location})


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
        text = scope_value_text(value)
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
