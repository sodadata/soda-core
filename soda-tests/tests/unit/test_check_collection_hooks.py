"""Test that the CheckCollection seam routes to subclass-provided types.

Concrete subtypes wire their YAML / impl / result / session-result types
via ``Generic[…]`` subscription at class-declaration time. The base classes'
``__init_subclass__`` derives the ClassVar hooks (``_YAML_CLASS``,
``_IMPL_CLASS``, ``_SESSION_RESULT_CLASS``, ``_RESULT_CLASS``) from
``cls.__orig_bases__`` via ``get_args``. The YAML side uses Python's
classmethod dispatch (``CheckCollectionYaml.parse()`` returns ``cls(...)``)
and needs no hook.

Sentinel subclasses below subscribe the base with DIFFERENT types than the
Contract* defaults, then assert that ``execute()`` and ``parse()`` return
instances of those sentinel types. This documents the extension recipe for
future check-collection subtypes shipped as soda-extensions packages.
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
    """``CheckCollectionYaml.parse()`` returns the calling subclass's type.

    With Phase 2's ``cls(...)`` dispatch, calling ``_SentinelYaml.parse(...)``
    constructs a ``_SentinelYaml``. No class-attribute hook is needed; Python's
    classmethod dispatch handles routing.
    """
    source = CheckCollectionYamlSource.from_str(_MINIMAL_YAML)
    instance = _SentinelYaml.parse(check_collection_yaml_source=source)
    assert isinstance(instance, _SentinelYaml), f"Expected _SentinelYaml, got {type(instance).__name__}"


def test_session_impl_hooks_route_to_subclass_types():
    """Sentinel session impl returns sentinel session result with sentinel per-collection results.

    Exercises all four hook slots on ``CheckCollectionVerificationSessionImpl`` and
    ``CheckCollectionImpl`` end-to-end. The sentinel subclasses wire them via
    ``Generic[…]`` subscription; ``__init_subclass__`` derives the ClassVars from
    ``cls.__orig_bases__``.
    Resolution paths:
    - ``_SESSION_RESULT_CLASS`` — session_result_cls(...) in execute()
    - ``_YAML_CLASS`` — yaml_cls.parse(...) in _execute_locally()
    - ``_IMPL_CLASS`` — impl_cls(...) in _execute_locally()
    - ``_RESULT_CLASS`` (on the impl) — result_cls(...) in verify(), reached via
      ``impl.verify()`` from _execute_locally()

    Uses ``only_validate_without_execute=True`` so no live data source is needed.
    """
    source = CheckCollectionYamlSource.from_str(_MINIMAL_YAML)

    result = _SentinelSessionImpl.execute(
        check_collection_yaml_sources=[source],
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
    )

    assert isinstance(result, _SentinelSessionResult), f"Expected _SentinelSessionResult, got {type(result).__name__}"
    # Sanity: the session must contain at least one per-collection result so the
    # next assertion is actually checking something. If the verify path bailed
    # early before constructing a per-collection result, list iteration below
    # would silently pass.
    assert (
        len(result.check_collection_results) == 1
    ), f"Expected exactly 1 per-collection result, got {len(result.check_collection_results)}"
    for per_collection in result.check_collection_results:
        assert isinstance(
            per_collection, _SentinelResult
        ), f"Expected _SentinelResult per-collection, got {type(per_collection).__name__}"


# ---------------------------------------------------------------------------
# Negative-path tests for the Generic seam
#
# The three tests below pin invariants of ``__init_subclass__``'s ``get_args``
# derivation that the happy-path tests above don't exercise. Each test
# documents a specific failure mode so a future change to the derivation
# breaks one of these loudly rather than silently routing to the wrong type.
# ---------------------------------------------------------------------------


def test_subclass_without_generic_subscription_leaves_classvars_unset():
    """Plain inheritance (no ``Generic[…]`` subscription) must NOT silently wire ClassVars.

    A future subtype author who forgets the subscription should hit an
    immediate ``AttributeError`` from the un-set ClassVar at first use,
    not a misleading "everything looks fine" path that secretly inherits
    base defaults from an unrelated parent.
    """

    class _PlainImpl(CheckCollectionImpl):
        """No Generic[…] subscription — exercises the no-op branch in __init_subclass__."""

    # ``_RESULT_CLASS`` is declared without a default on the base — reading it on
    # a subclass that didn't subscribe Generic raises AttributeError, which is
    # the documented failure mode for un-wired bare subclasses.
    with pytest.raises(AttributeError):
        _PlainImpl._RESULT_CLASS  # noqa: B018 — intentional attribute access


def test_intermediate_typevar_subclass_leaves_classvars_unset_concrete_leaf_wires():
    """A subclass parameterized with TypeVars (an intermediate abstract subtype) is skipped by the derivation.

    The ``isinstance(arg, type)`` filter inside ``__init_subclass__`` ensures
    only concrete-type subscriptions populate the ClassVars. This allows
    intermediate abstract subtypes (e.g. a generic ``BaseScanImpl(CheckCollectionImpl[Y, R])``
    that further-specializes later) to exist without locking in any concrete
    type. The concrete leaf that subscribes with real types wires the hooks.
    """

    _IntermediateYaml = TypeVar("_IntermediateYaml", bound=CheckCollectionYaml)
    _IntermediateResult = TypeVar("_IntermediateResult", bound=CheckCollectionResult)

    class _IntermediateImpl(CheckCollectionImpl[_IntermediateYaml, _IntermediateResult]):
        """TypeVar-parameterized — abstract, no ClassVar wiring."""

    # Intermediate subscribed with TypeVars: derivation skipped (TypeVar instances are
    # not ``type`` and the ``isinstance(arg, type)`` filter excludes them).
    with pytest.raises(AttributeError):
        _IntermediateImpl._RESULT_CLASS  # noqa: B018

    # Now a concrete leaf subscribing with real types DOES wire the ClassVar.
    class _LeafResult(CheckCollectionResult):
        pass

    class _LeafYaml(CheckCollectionYaml):
        pass

    class _LeafImpl(_IntermediateImpl[_LeafYaml, _LeafResult]):
        """Concrete leaf — derivation populates ClassVars from these args."""

    assert _LeafImpl._RESULT_CLASS is _LeafResult


def test_session_impl_with_typevar_arg_skips_only_that_slot():
    """If one Generic arg is a TypeVar and the others are concrete, only the concrete slots wire.

    Mixed-concrete-and-TypeVar subscriptions are unusual but legal. The
    derivation must wire whatever concrete types it can and leave the
    TypeVar-typed slots unset (so a further-specialized leaf can subscribe
    them later).
    """

    _MixedYaml = TypeVar("_MixedYaml", bound=CheckCollectionYaml)

    class _MixedSessionResult(CheckCollectionSessionResult):
        pass

    class _MixedImpl(CheckCollectionImpl[CheckCollectionYaml, CheckCollectionResult]):
        pass

    class _MixedSessionImpl(CheckCollectionVerificationSessionImpl[_MixedYaml, _MixedImpl, _MixedSessionResult]):
        """First arg is a TypeVar — _YAML_CLASS stays unset, _IMPL_CLASS and _SESSION_RESULT_CLASS wire."""

    # Concrete slots are wired.
    assert _MixedSessionImpl._IMPL_CLASS is _MixedImpl
    assert _MixedSessionImpl._SESSION_RESULT_CLASS is _MixedSessionResult
    # TypeVar slot stays unset — the derivation skipped it via isinstance(arg, type) filter.
    with pytest.raises(AttributeError):
        _MixedSessionImpl._YAML_CLASS  # noqa: B018


# ---------------------------------------------------------------------------
# ClassVar identity recipe: _DISPLAY_NAME / _KIND / _WIRE_SOURCE
#
# Every concrete check-collection subtype declares three identity ClassVars
# with distinct purposes:
#   * ``_DISPLAY_NAME`` — user-facing word (e.g. "contract").
#   * ``_KIND`` — machine identifier used as the YAML ``kind:`` discriminator
#     and registry key (e.g. "contract").
#   * ``_WIRE_SOURCE`` — per-check wire ``source`` value the Cloud backend
#     receives (e.g. "soda-contract"); declared on the impl, not the yaml.
#
# The recipe keeps the three concepts separate so subtypes whose wire
# identifier differs from their user-facing word can diverge the two
# strings, even though for contracts they both equal "contract".
# ---------------------------------------------------------------------------


def test_kind_classvar_distinct_from_display_name():
    """``_KIND`` is the wire identifier; ``_DISPLAY_NAME`` is the user-facing word.

    They happen to be the same string for ContractYaml because the wire
    identifier and the user word collide. Declared separately so other
    subtypes can diverge the two.
    """
    # Abstract base default values differ — ``_DISPLAY_NAME`` is multi-word
    # ("check collection") while ``_KIND`` is snake_case ("check_collection").
    assert CheckCollectionYaml._DISPLAY_NAME == "check collection"
    assert CheckCollectionYaml._KIND == "check_collection"

    # ContractYaml: both happen to equal "contract" today.
    assert ContractYaml._DISPLAY_NAME == "contract"
    assert ContractYaml._KIND == "contract"
    assert ContractYaml._KIND == ContractYaml._DISPLAY_NAME


def test_wire_source_classvar_on_contract_impl():
    """``_WIRE_SOURCE`` is declared on the impl, not the yaml.

    The cloud upload path reads ``type(check_collection_impl)._WIRE_SOURCE``
    when stamping the per-check ``source`` field. Today's only wire value
    is ``"soda-contract"``; other subtypes that ship as soda-extensions
    packages set their own values.
    """
    assert ContractImpl._WIRE_SOURCE == "soda-contract"


def test_wire_source_required_on_concrete_subclass():
    """The abstract base does not declare ``_WIRE_SOURCE``; reading raises.

    A misconfigured subtype that forgets to set ``_WIRE_SOURCE`` fails loud
    at first read instead of silently uploading checks under the abstract
    base's identifier.
    """
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckCollectionImpl,
    )

    with pytest.raises(AttributeError):
        CheckCollectionImpl._WIRE_SOURCE  # noqa: B018


def test_extra_identity_properties_default_is_empty():
    """Default ``_extra_identity_properties`` returns an empty dict.

    Contracts contribute no extra inputs to the identity hash → existing
    contract identities stay byte-identical post-refactor. The hook only
    exists so Phase 2's non-contract subtypes can append ``collection_name``
    without scattering ``if collection_name is not None`` checks across the
    base.
    """
    # Sentinel CheckImpl-shaped object exercising only the hook method.
    # Constructing a real CheckImpl from a YAML is heavy; we only need the
    # method's return value here.
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    assert CheckImpl._extra_identity_properties.__doc__ is not None

    # The default returns {} — call the unbound method via a minimal mock self
    # so we exercise the body, not just the signature.
    class _Probe:
        _extra_identity_properties = CheckImpl._extra_identity_properties

    assert _Probe()._extra_identity_properties() == {}


def test_merge_identity_properties_returns_none_when_both_empty():
    """When the hook is empty and no explicit dict is given, merge returns None.

    The ``None`` short-circuit preserves the existing call shape into
    ``_build_identity`` — passing ``None`` vs. ``{}`` is observable inside the
    classmethod (it checks ``if extra_identity_properties:``).
    """
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    class _Probe:
        _extra_identity_properties = CheckImpl._extra_identity_properties
        _merge_identity_properties = CheckImpl._merge_identity_properties

    assert _Probe()._merge_identity_properties(None) is None
    assert _Probe()._merge_identity_properties({}) is None


def test_merge_identity_properties_explicit_wins_over_hook():
    """Explicit constructor-supplied entries override hook contributions on key collision.

    Caller intent is authoritative; the hook is a base-level default that
    subtypes can override but constructor callers always win.
    """
    from soda_core.check_collections.impl.check_collection_verification_impl import (
        CheckImpl,
    )

    class _Probe:
        _merge_identity_properties = CheckImpl._merge_identity_properties

        def _extra_identity_properties(self) -> dict[str, object]:
            return {"cn": "hook-value", "extra": "kept"}

    result = _Probe()._merge_identity_properties({"cn": "caller-value"})
    assert result == {"cn": "caller-value", "extra": "kept"}
