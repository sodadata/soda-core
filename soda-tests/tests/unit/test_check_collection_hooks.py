"""Pins the impl-side seams subtypes plug into on ``CheckCollectionImpl``
and ``CheckCollectionYaml``: Generic subscription, identity ClassVars,
and ``CheckImpl`` identity hooks.

The session-side Generic derivation that used to live here was removed in
the DES-315 reshape — the session impl now dispatches via the family
registry per spec rather than via ``Generic[YamlT, ImplT, ResultT]`` on a
session subclass. See ``test_session_dispatch.py`` for the replacement
coverage of mixed-kind sessions, unknown-kind errors, and agent dispatch.
"""

from typing import TypeVar

import pytest
from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml


class _SentinelYaml(CheckCollectionYaml):
    """Plain inheritance — parse() dispatches via cls."""


class _SentinelResult(CheckCollectionResult):
    """Sentinel per-collection result."""


class _SentinelImpl(CheckCollectionImpl[_SentinelYaml, _SentinelResult]):
    """Sentinel impl — wires _RESULT_CLASS via Generic[] subscription."""

    _WIRE_SOURCE = "soda-sentinel"


_MINIMAL_YAML = """\
dataset: datasource/db/schema/orders
columns:
  - name: id
    checks:
      - missing:
"""


def test_yaml_class_dispatch_routes_to_subclass():
    """``CheckCollectionYaml.parse()`` returns the calling subclass's type."""
    source = CheckCollectionYamlSource.from_str(_MINIMAL_YAML)
    instance = _SentinelYaml.parse(check_collection_yaml_source=source)
    assert isinstance(instance, _SentinelYaml), f"Expected _SentinelYaml, got {type(instance).__name__}"


# Negative paths on ``__init_subclass__``'s ``get_args`` derivation — each
# test pins one failure mode so a future change breaks loud.


def test_subclass_without_generic_subscription_leaves_classvars_unset():
    """Plain inheritance: ``_RESULT_CLASS`` stays unset → AttributeError on read."""

    class _PlainImpl(CheckCollectionImpl):
        pass

    with pytest.raises(AttributeError):
        _PlainImpl._RESULT_CLASS  # noqa: B018


def test_intermediate_typevar_subclass_leaves_classvars_unset_concrete_leaf_wires():
    """TypeVar subscription: ``isinstance(arg, type)`` filter skips; concrete leaf wires."""

    _IntermediateYaml = TypeVar("_IntermediateYaml", bound=CheckCollectionYaml)
    _IntermediateResult = TypeVar("_IntermediateResult", bound=CheckCollectionResult)

    class _IntermediateImpl(CheckCollectionImpl[_IntermediateYaml, _IntermediateResult]):
        pass

    with pytest.raises(AttributeError):
        _IntermediateImpl._RESULT_CLASS  # noqa: B018

    class _LeafResult(CheckCollectionResult):
        pass

    class _LeafYaml(CheckCollectionYaml):
        pass

    class _LeafImpl(_IntermediateImpl[_LeafYaml, _LeafResult]):
        _WIRE_SOURCE = "soda-leaf"

    assert _LeafImpl._RESULT_CLASS is _LeafResult


# Subtype identity ClassVars: ``_KIND`` (wire id) and ``_DISPLAY_NAME`` (user
# word) on the YAML; ``_WIRE_SOURCE`` (per-check Cloud ``source``) on the
# impl. For contracts ``_KIND == _DISPLAY_NAME == "contract"``; subtypes may
# diverge them.


def test_kind_classvar_distinct_from_display_name():
    """``_KIND`` is the wire id; ``_DISPLAY_NAME`` is the user-facing word."""
    assert CheckCollectionYaml._DISPLAY_NAME == "check collection"
    assert CheckCollectionYaml._KIND == "check_collection"

    assert ContractYaml._DISPLAY_NAME == "contract"
    assert ContractYaml._KIND == "contract"
    assert ContractYaml._KIND == ContractYaml._DISPLAY_NAME


def test_wire_source_classvar_on_contract_impl():
    """Cloud upload reads ``type(impl)._WIRE_SOURCE``; contracts use ``"soda-contract"``."""
    assert ContractImpl._WIRE_SOURCE == "soda-contract"


def test_wire_source_required_on_concrete_subclass():
    """Bare-base read raises AttributeError (no default — subtypes MUST set it)."""
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckCollectionImpl,
    )

    with pytest.raises(AttributeError):
        CheckCollectionImpl._WIRE_SOURCE  # noqa: B018


def test_classvar_validator_rejects_concrete_subtype_without_wire_source():
    """A concrete subtype declared without ``_WIRE_SOURCE`` fails at class creation."""

    class _MissingResult(CheckCollectionResult):
        pass

    class _MissingYaml(CheckCollectionYaml):
        pass

    with pytest.raises(TypeError, match="must declare _WIRE_SOURCE"):

        class _MissingWireSourceImpl(CheckCollectionImpl[_MissingYaml, _MissingResult]):
            pass


def test_extra_identity_properties_default_is_empty():
    """Default hook returns ``{}`` so contract identity hashes stay byte-identical."""
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    class _Probe:
        _extra_identity_properties = CheckImpl._extra_identity_properties

    assert _Probe()._extra_identity_properties() == {}


def test_merge_identity_properties_returns_none_when_both_empty():
    """Both empty → merge returns ``None`` so ``_build_identity`` call shape is unchanged."""
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    class _Probe:
        _extra_identity_properties = CheckImpl._extra_identity_properties
        _merge_identity_properties = CheckImpl._merge_identity_properties

    assert _Probe()._merge_identity_properties(None) is None
    assert _Probe()._merge_identity_properties({}) is None


def test_merge_identity_properties_explicit_wins_over_hook():
    """Caller-supplied keys override hook contributions on collision."""
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    class _Probe:
        _merge_identity_properties = CheckImpl._merge_identity_properties

        def _extra_identity_properties(self) -> dict[str, object]:
            return {"cn": "hook-value", "extra": "kept"}

    result = _Probe()._merge_identity_properties({"cn": "caller-value"})
    assert result == {"cn": "caller-value", "extra": "kept"}
