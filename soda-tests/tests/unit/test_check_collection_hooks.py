"""Test that the CheckCollection seam routes to subclass-provided types.

Concrete subtypes subscribe ``Generic[…]`` and pass ``yaml_type=`` / ``impl_type=`` /
``session_result_type=`` (on the session impl) and ``result_type=`` (on the impl)
via ``__init_subclass__`` at class-declaration time. The YAML side uses Python's
classmethod dispatch — ``CheckCollectionYaml.parse()`` does ``return cls(...)`` —
so the yaml subclass needs no special wiring.

Sentinel subclasses below wire these hooks to a DIFFERENT set of types than the
Contract* defaults, then assert that ``execute()`` and ``parse()`` return
instances of the sentinel types. This documents the extension recipe for future
check-collection subtypes (data-standards, check-suites, etc.).
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


class _SentinelImpl(
    CheckCollectionImpl[_SentinelYaml, _SentinelResult],
    result_type=_SentinelResult,
):
    """Sentinel impl — wires _RESULT_CLASS via __init_subclass__."""


class _SentinelSessionImpl(
    CheckCollectionVerificationSessionImpl[_SentinelYaml, _SentinelImpl, _SentinelSessionResult],
    yaml_type=_SentinelYaml,
    impl_type=_SentinelImpl,
    session_result_type=_SentinelSessionResult,
):
    """Sentinel session impl — wires three hooks via __init_subclass__ kwargs."""


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
    ``__init_subclass__`` kwargs (``yaml_type=`` / ``impl_type=`` /
    ``session_result_type=`` on the session impl; ``result_type=`` on the impl).
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
