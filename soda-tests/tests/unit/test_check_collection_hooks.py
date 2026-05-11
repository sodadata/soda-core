"""Test that the CheckCollection* polymorphism hooks route to subclass-provided
types when a subclass wires them.

This addresses F7 from the Phase 1 review (commit 60126325). The four
class-attribute hooks (``_YAML_CLASS``, ``_IMPL_CLASS``, ``_RESULT_CLASS``,
``_SESSION_RESULT_CLASS``) defaulted to ``Contract*`` types so the existing
suite passes unchanged; that same suite cannot distinguish "the hooks route
to subclass types" from "the code hard-codes Contract* types" because the
Contract* path sets the hooks to itself. The tests below define sentinel
subclasses that wire the hooks to a DIFFERENT set of types and assert that
execute() / parse() return instances of those sentinel types.

Documents the extension recipe for future check-collection subtypes
(data-standards, check-suites, etc.).
"""

from typing import Optional

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
    """Sentinel YAML subclass — proves _YAML_CLASS routing."""


class _SentinelResult(CheckCollectionResult):
    """Sentinel per-collection result subclass — proves _RESULT_CLASS routing."""


class _SentinelSessionResult(CheckCollectionSessionResult):
    """Sentinel session-result subclass — proves _SESSION_RESULT_CLASS routing.

    Accepts the legacy ``contract_verification_results`` keyword used by
    ``CheckCollectionVerificationSessionImpl.execute()`` at the
    ``session_result_cls(contract_verification_results=...)`` construction
    site, mirroring ``ContractVerificationSessionResult``.
    """

    def __init__(
        self,
        contract_verification_results: Optional[list[CheckCollectionResult]] = None,
        check_collection_results: Optional[list[CheckCollectionResult]] = None,
    ):
        results = (
            contract_verification_results
            if contract_verification_results is not None
            else check_collection_results
        )
        super().__init__(check_collection_results=results)


class _SentinelImpl(CheckCollectionImpl):
    """Sentinel impl subclass — proves _IMPL_CLASS routing.

    ``_RESULT_CLASS`` on the impl drives what ``verify()`` constructs for the
    per-collection result. We point it at ``_SentinelResult`` so the
    per-collection assertion in the session test can distinguish sentinel
    routing from the default ContractVerificationResult fallback.
    """

    _RESULT_CLASS = _SentinelResult


class _SentinelSessionImpl(CheckCollectionVerificationSessionImpl):
    """Sentinel session-impl subclass — wires all four hooks to sentinel types."""

    _YAML_CLASS = _SentinelYaml
    _IMPL_CLASS = _SentinelImpl
    _RESULT_CLASS = _SentinelResult
    _SESSION_RESULT_CLASS = _SentinelSessionResult


# Wire the YAML class hook on the sentinel YAML subclass itself, mirroring how
# ``ContractYaml._YAML_CLASS = ContractYaml`` is wired in contract_yaml.py.
# Without this, ``_SentinelYaml.parse(...)`` would fall through to the default
# (ContractYaml) instead of constructing a _SentinelYaml.
_SentinelYaml._YAML_CLASS = _SentinelYaml


_MINIMAL_YAML = """\
dataset: datasource/db/schema/orders
columns:
  - name: id
    checks:
      - missing:
"""


def test_yaml_class_hook_routes_to_subclass():
    """``CheckCollectionYaml.parse()`` called via the sentinel subclass returns the sentinel type.

    Proves the ``_YAML_CLASS`` hook on ``CheckCollectionYaml`` itself routes to
    the subclass type. The Contract* path sets the hook to ContractYaml; a
    sentinel subclass that sets the hook to itself must receive its own type
    from ``parse()``, not the default ContractYaml.
    """
    source = CheckCollectionYamlSource.from_str(_MINIMAL_YAML)
    instance = _SentinelYaml.parse(check_collection_yaml_source=source)
    assert isinstance(instance, _SentinelYaml), (
        f"Expected _SentinelYaml, got {type(instance).__name__}"
    )


def test_session_impl_hooks_route_to_subclass_types():
    """Sentinel session impl returns sentinel session result with sentinel per-collection results.

    Exercises all four hooks on ``CheckCollectionVerificationSessionImpl`` end-to-end:
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

    assert isinstance(result, _SentinelSessionResult), (
        f"Expected _SentinelSessionResult, got {type(result).__name__}"
    )
    # Sanity: the session must contain at least one per-collection result so the
    # next assertion is actually checking something. If the verify path bailed
    # early before constructing a per-collection result, list iteration below
    # would silently pass.
    assert len(result.check_collection_results) == 1, (
        f"Expected exactly 1 per-collection result, got {len(result.check_collection_results)}"
    )
    for per_collection in result.check_collection_results:
        assert isinstance(per_collection, _SentinelResult), (
            f"Expected _SentinelResult per-collection, got {type(per_collection).__name__}"
        )
