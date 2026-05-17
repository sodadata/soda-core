"""Pins the seams subtypes plug into: Generic subscription, ClassVars,
override hooks. Sentinel subclasses subscribe the base with non-Contract*
types and assert ``execute()`` / ``parse()`` return those types.
"""

from typing import TypeVar

import pytest
from soda_core.check_collections.check_collection_verification import (
    CheckCollectionResult,
    CheckCollectionSessionResult,
)
from soda_core.check_collections.impl.check_collection_verification_impl import (
    CheckCollectionImpl,
    CheckCollectionVerificationSessionImpl,
)
from soda_core.check_collections.impl.check_collection_yaml import CheckCollectionYaml
from soda_core.common.yaml import CheckCollectionYamlSource
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml


class _SentinelYaml(CheckCollectionYaml):
    """Plain inheritance — parse() dispatches via cls."""


class _SentinelResult(CheckCollectionResult):
    """Sentinel per-collection result."""


class _SentinelSessionResult(CheckCollectionSessionResult):
    """Sentinel session result."""


class _SentinelImpl(CheckCollectionImpl[_SentinelYaml, _SentinelResult]):
    """Sentinel impl — wires _RESULT_CLASS via Generic[] subscription."""


class _SentinelSessionImpl(
    CheckCollectionVerificationSessionImpl[_SentinelYaml, _SentinelImpl, _SentinelSessionResult]
):
    """Sentinel session impl — wires three hooks via Generic[] subscription."""


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


def test_session_impl_hooks_route_to_subclass_types():
    """All four hook slots resolve to the sentinel types via Generic subscription."""
    source = CheckCollectionYamlSource.from_str(_MINIMAL_YAML)

    result = _SentinelSessionImpl.execute(
        check_collection_yaml_sources=[source],
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert isinstance(result, _SentinelSessionResult), f"Expected _SentinelSessionResult, got {type(result).__name__}"
    # Sanity check that at least one per-collection result exists so the
    # type assertion below isn't vacuously true.
    assert (
        len(result.check_collection_results) == 1
    ), f"Expected exactly 1 per-collection result, got {len(result.check_collection_results)}"
    for per_collection in result.check_collection_results:
        assert isinstance(
            per_collection, _SentinelResult
        ), f"Expected _SentinelResult per-collection, got {type(per_collection).__name__}"


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
        pass

    assert _LeafImpl._RESULT_CLASS is _LeafResult


def test_session_impl_with_typevar_arg_skips_only_that_slot():
    """Mixed concrete/TypeVar subscription: concrete slots wire, TypeVar slot stays unset."""

    _MixedYaml = TypeVar("_MixedYaml", bound=CheckCollectionYaml)

    class _MixedSessionResult(CheckCollectionSessionResult):
        pass

    class _MixedImpl(CheckCollectionImpl[CheckCollectionYaml, CheckCollectionResult]):
        pass

    class _MixedSessionImpl(CheckCollectionVerificationSessionImpl[_MixedYaml, _MixedImpl, _MixedSessionResult]):
        pass

    assert _MixedSessionImpl._IMPL_CLASS is _MixedImpl
    assert _MixedSessionImpl._SESSION_RESULT_CLASS is _MixedSessionResult
    with pytest.raises(AttributeError):
        _MixedSessionImpl._YAML_CLASS  # noqa: B018


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
