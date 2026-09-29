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
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from soda_core.common.filtered_cte import filtered_cte_alias
from soda_core.common.logs import Location
from soda_core.common.yaml import YamlList, YamlObject, YamlSource

if TYPE_CHECKING:
    from soda_core.common.sql_ast import CTE
    from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl

BASE_SCOPE_KEY: str = "base"
RESERVED_SCOPE_KEYS: frozenset[str] = frozenset({"base", "true", "false", "null", "yes", "no", "on", "off", "y", "n"})
SCOPE_KEY_PATTERN: re.Pattern[str] = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


def mark_scope_support(yaml_source: YamlSource, supports_scopes: bool) -> None:
    """Records on ``yaml_source`` whether the kind of its file supports scopes.

    Reads of scope input from that file resolve variables only when it does, so a kind without support never logs
    or fails on its scope input. ``ContractYaml`` records it before it reads any. A file nobody recorded it for
    reads its scope input as written.
    """
    yaml_source._supports_scopes = supports_scopes


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
