"""Lock the wire ``checkPath`` emission format for each check-collection subtype.

The backend's ``CheckCollectionModule.firstSegmentOf(checkPath)`` splits on
the first ``.`` and matches the first segment verbatim against
``DataStandard.name``. The format is therefore pinned at the wire boundary:

- Contracts: ``checkPath`` is the yaml-internal stripped ``Check.path``.
  Byte-identical to every prior contract verification.
- Non-contract subtypes (e.g. data standards): ``checkPath`` is prefixed
  with ``"{collection_id}.{path}"`` so the backend filter routes it.

This test exercises ``_build_check_result_cloud_dict`` — the only wire-bound
site reading ``Check.full_path`` — and the upstream ``Check.full_path``
property on ``CheckImpl`` for both branches of the wire_source heuristic.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from soda_core.common.logs import Location
from soda_core.common.soda_cloud import _build_check_result_cloud_dict
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
    YamlFileContentInfo,
)


def _make_contract() -> Contract:
    return Contract(
        data_source_name="test_ds",
        dataset_prefix=["some", "schema"],
        dataset_name="CUSTOMERS",
        soda_qualified_dataset_name="test_ds/some/schema/CUSTOMERS",
        source=YamlFileContentInfo(source_content_str=None, local_file_path="fake.yml"),
    )


def _make_check(path: str, full_path: str) -> Check:
    return Check(
        column_name="email",
        type="missing",
        qualifier=None,
        name="No missing values",
        path=path,
        full_path=full_path,
        identity="abcd1234",
        definition="missing: ...",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes={},
        location=Location(file_path="fake.yml", line=1, column=1),
    )


def _make_check_result(path: str, full_path: str) -> CheckResult:
    return CheckResult(
        check=_make_check(path=path, full_path=full_path),
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def test_contract_check_path_is_unprefixed():
    """Contract subtype: ``checkPath`` is the bare ``Check.path`` (today's format).

    Byte-identical to every previous contract verification — Cloud history
    keyed off this value must not break.
    """
    path = "columns.email.checks.missing"
    check_result = _make_check_result(path=path, full_path=path)
    wire = _build_check_result_cloud_dict(
        contract=_make_contract(),
        check_result=check_result,
        wire_source="soda-contract",
    )
    assert wire["checkPath"] == path
    assert wire["source"] == "soda-contract"


def test_data_standard_check_path_is_prefixed_with_collection_name():
    """Non-contract subtype: ``checkPath`` is ``"{collection_id}.{path}"``.

    Backend's ``firstSegmentOf(checkPath)`` splits on the first ``.`` and
    looks up ``DataStandard`` by the first segment. The collection_id
    (POC convention: matches ``DataStandard.name``) is the prefix.
    """
    path = "columns.email.checks.missing"
    full_path = f"my_pii_standard.{path}"
    check_result = _make_check_result(path=path, full_path=full_path)
    wire = _build_check_result_cloud_dict(
        contract=_make_contract(),
        check_result=check_result,
        wire_source="data-standard",
    )
    assert wire["checkPath"] == "my_pii_standard.columns.email.checks.missing"
    assert wire["source"] == "data-standard"


class _StubCheckImpl:
    """Local sentinel that mimics the surface ``CheckImpl.full_path`` reads.

    We don't subclass ``CheckImpl`` because the real ``__init__`` requires a
    full ``ContractImpl`` (and the real ``path`` property reads
    ``column_impl.column_yaml.name``). The property under test only reads
    ``self.path``, ``self.contract_impl.wire_source``, and
    ``self.contract_impl.collection_id`` — we mirror those exactly.
    """

    # Borrow the production property verbatim so any future refactor that
    # adds branches is exercised by these tests.
    from soda_core.contracts.impl.contract_verification_impl import (  # noqa: E402
        CheckImpl as _RealCheckImpl,
    )

    full_path = _RealCheckImpl.full_path

    def __init__(self, *, wire_source: str, collection_id, path: str):
        self.path = path
        self.contract_impl = MagicMock()
        self.contract_impl.wire_source = wire_source
        self.contract_impl.collection_id = collection_id


def test_check_full_path_property_for_contract_subtype_returns_bare_path():
    """``CheckImpl.full_path`` returns ``self.path`` when the parent has
    ``wire_source == "soda-contract"`` — regardless of any collection_id
    that might be set on the contract.
    """
    stub = _StubCheckImpl(
        wire_source="soda-contract",
        collection_id="some_id_we_ignore",
        path="columns.age.checks.missing",
    )
    assert stub.full_path == "columns.age.checks.missing"


def test_check_full_path_property_for_data_standard_subtype_prefixes_with_collection_id():
    """``CheckImpl.full_path`` returns ``f"{collection_id}.{path}"`` when
    the parent has ``wire_source != "soda-contract"`` and ``collection_id``
    is set.
    """
    stub = _StubCheckImpl(
        wire_source="data-standard",
        collection_id="my_pii_standard",
        path="columns.age.checks.missing",
    )
    assert stub.full_path == "my_pii_standard.columns.age.checks.missing"


def test_check_full_path_for_hypothetical_third_wire_source_also_prefixes():
    """Any future non-contract ``wire_source`` literal (not ``"soda-contract"``)
    triggers prefixing — the heuristic isn't ``"data-standard"`` specifically.
    """
    stub = _StubCheckImpl(
        wire_source="some-future-source",
        collection_id="my_collection",
        path="checks.row_count",
    )
    assert stub.full_path == "my_collection.checks.row_count"


def test_check_full_path_falls_back_to_bare_path_when_collection_id_missing():
    """Defensive fallback: if a non-contract impl exposes a Check without
    a ``collection_id`` (the engine guard in ``verify()`` normally prevents
    this), ``full_path`` falls back to the bare ``path`` rather than emitting
    a malformed ``"None.<path>"`` string. The engine's ``verify()`` raises
    before this property is reached during a real run — this fallback only
    matters for synthetic Check construction paths (e.g. ``build_error_result``).
    """
    stub = _StubCheckImpl(
        wire_source="data-standard",
        collection_id=None,
        path="checks.row_count",
    )
    assert stub.full_path == "checks.row_count"


def test_verify_raises_when_non_contract_impl_missing_collection_id():
    """The engine-level guard: non-contract subtypes must declare a
    ``collection_id``. Without it the wire ``checkPath`` would be bare and
    the backend filter would drop every check. ``verify()`` raises early.
    """
    from soda_core.check_collections.base import (
        CheckCollectionImpl,
        CheckCollectionResult,
        CheckCollectionYaml,
    )

    class _NoCollectionIdImpl(CheckCollectionImpl):
        wire_source = "data-standard"
        display_name = "data-standard-no-id"
        yaml_class = CheckCollectionYaml
        result_class = CheckCollectionResult

        def __init__(self):
            # Skip the heavy engine init — the guard fires before any work.
            # ``collection_id`` defaults to ``None`` via the base class's
            # read-only @property — no instance attribute needed.
            self.wire_source = "data-standard"

    with pytest.raises(ValueError, match="collection_id"):
        _NoCollectionIdImpl().verify()
