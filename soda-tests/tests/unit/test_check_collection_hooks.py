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
future check-collection subtypes (data-standards, check-suites, etc.).
"""

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
